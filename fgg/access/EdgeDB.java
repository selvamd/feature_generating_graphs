package fgg.access;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement; 
import java.sql.ResultSet;
import java.sql.Blob;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.*;
import java.nio.*;

import fgg.access.*;
import fgg.data.*;
import fgg.utils.*;

public class EdgeDB extends Persistor {

    private Connection conn; 
    private LinkType type; 
    private String table; 
    private Map<Integer,Edge> edges;
    private int maxkey = 0;
    
    public EdgeDB(LinkType type) {
        this.type = type;
        this.table = "e_"+type.toString().toLowerCase();
        this.edges = new HashMap<Integer,Edge>();
        try {
            conn = getConnectionWithRetries(table);
            createTable();
            createIndex("k0");
            createIndex("k1");
            maxkey = maxkey();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public int nextkey() { return maxkey+1; }
    public int batchSize() { return edges.size(); }

    //Returns component objkey for a given linkkey value
	public synchronized int objkey(CBOType ctype, int nodecnt, int pk) {
        Edge edge = edges.get(pk);
        if (edge == null) edge = findByPk(pk);
        if (edge == null) return -1;
        int idx = type.cboindex(ctype,nodecnt);
        return (idx >= 0)? edge.key[idx]:-1;
    }

    //Returns a pair of <objkeypk,obkkeyfk> for a given ctype (only for 2 node links)
	public synchronized Set<Long> linkedkey(CBOType ctype, int asofdt, Set<Long> out) {
        return linkedkey(ctype, asofdt, 0, out);
    }
    
    //Returns a pair of <objkeypk,obkkeyfk> for a given ctype (only for 2 node links)
	public synchronized Set<Long> linkedkey(CBOType ctype, int count, int asofdt, Set<Long> out) 
    {
		out.clear();
        if (type.maxnodes() != 2) return out;
        int idx = type.cboindex(ctype, count);
        if (idx == -1) return out;
        return readLink(idx, (idx+1)%2, asofdt, out);
    }

    //Returns matching linkkeys given partial or full component objkeys
    //If includeObj=TRUE, returns component objkeys are included in the result
	public synchronized Set<Integer> read(int[] keys, boolean includeObj, Set<Integer> out)
    {
		out.clear();
        read(keys, keys, 0, includeObj, out);
        for (Edge e:edges.values()) {
            if (e.matchKey(keys)) {
                out.add(e.lk);
                if (includeObj)
                    for (int k:e.key)
                        out.add(k);
            }
        }
        return out;
    }
    
    //Creates or looks up a linkkey given component objkeys
    public synchronized int upsert(int[] keys, int fromdt, int todt) 
    {
        for (int i=0;i<type.maxnodes();i++)
            if (keys[i]<=0) 
                return -1;
            
        Edge edge = findByPk(keys);
        if (edge == null) 
        {
            edge = new Edge();
            edge.lk = nextkey();
            edge.key = new int[5];
            for (int i=0;i<keys.length;i++)
                edge.key[i] = keys[i];
        }
        edge.addTime(fromdt, todt);
        maxkey = Math.max(maxkey,edge.lk);
        edges.put(edge.lk,edge);
        if (edges.size() > 500) 
        {
            try { 
                flush(); 
                edges.clear();
            } catch (Exception e) {
            }
        }
        return edge.lk;
    }

    public synchronized void flush() throws Exception 
    {
        String sql = "INSERT or replace INTO " + table + " VALUES(?,?,?,?,?,?,?)";
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (Edge edge:edges.values()) 
        {
            pstmt.setInt(1,edge.lk);
            for (int i=0;i<edge.key.length;i++)
                pstmt.setInt(2+i,edge.key[i]);
            ByteBuffer buff = ByteBuffer.allocate(edge.dt.length * 4 * 2);
            for (int i=0;i<edge.dt.length;i++) 
                buff.putLong(edge.dt[i]);
            pstmt.setBytes(7,buff.array());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        conn.commit();
        pstmt.close();
        edges.clear();
    }

    /////////////////////////////////////// Private methods /////////////////////////////
	//Expects "from" and "to" to represent a range [from -> to] of keys to filter and include
	//Make "from" or "to" negative to indicate exclusiveness at the boundaries
	//Make "from" or "to" 0 to indicate open boundary or skip checking
	//For exact match, make from and to be the same positive value
	private Set<Integer> read(int[] from, int[] to, int asofdt, boolean includeObj, Set<Integer> out)
	{
        String sql = readSql(from,to);
		try {
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                if (asofdt > 0) {
                    boolean status = false;
                    Blob blob = rs.getBlob("dt");
                    if (blob != null && blob.length() > 0) {
                        ByteBuffer buff = ByteBuffer.wrap(blob.getBytes(1,(int)blob.length()));
                        while (buff.hasRemaining() && asofdt >= buff.getInt()) 
                            status = !status;
                        if (!status) continue;
                    }
                }
                out.add(rs.getInt("lk"));
                if (includeObj)
                    for (int i=0;i<type.maxnodes();i++)
                        out.add(rs.getInt("k"+i));
                    
            }
            rs.close(); 
            stmt.close();
		} catch (Exception e) {
            e.printStackTrace();
		}
		return out;
	}

	private Set<Long> readLink(int pkidx, int fkidx, int asofdt, Set<Long> out)
	{
        String sql = readSql(null,null);
		try {
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                if (asofdt > 0) {
                    boolean status = false;
                    Blob blob = rs.getBlob("dt");
                    if (blob != null && blob.length() > 0) {
                        ByteBuffer buff = ByteBuffer.wrap(blob.getBytes(1,(int)blob.length()));
                        while (buff.hasRemaining()) { 
                            long l =  buff.getLong();
                            if (asofdt >= Utils.msb(l) && asofdt <= Utils.lsb(l))
                                status = true;
                        }
                        if (!status) continue;
                    }
                }
                out.add(Utils.makelong(rs.getInt(pkidx),rs.getInt(fkidx)));
            }
            rs.close(); 
            stmt.close();
		} catch (Exception e) {
            e.printStackTrace();
		}
		return out;
	}

    private String readSql(int[] from, int[] to) 
    {
        String filter = "";
        if (from != null)
            for (int i = 0;i < from.length; i++)
                if (from[i] > 0)
                    filter += " AND k" + i + " >= " + from[i];
                else if (from[i] < 0)   
                    filter += " AND k" + i + " > " + (-1 * from[i]);
                
        if (to != null) 
            for (int i = 0;i < to.length; i++)
                if (to[i] > 0)
                    filter += " AND k" + i + " <= " + to[i];
                else if (to[i] < 0)
                    filter += " AND k" + i + " < " + (-1 * to[i]);
            
        String sql = "select * from " + table;
        if (filter.startsWith(" AND")) sql += " where " + filter.substring(4);
        return sql;
    }
    
    private int maxkey() throws Exception {
        int key = 100;
        String sql = "select max(lk) from " + table;
        Statement stmt  = conn.createStatement();
        ResultSet rs    = stmt.executeQuery(sql);
        if (rs.next()) key = rs.getInt(1);
        stmt.close();
        rs.close();
        return key;
    }

    private Edge findByPk(int[] keys) 
    {
        Edge edge = null;
        for (Edge e:edges.values()) {
            if (e.matchKey(keys)) {
                edge = e; 
                break;
            }
        }
        if (edge != null) return edge;
        try {
            String sql = readSql(keys,keys);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                edge = new Edge();
                edge.init(rs);
            }
            rs.close(); 
            stmt.close();
        } catch (Exception e) {
        }
        return edge;
    }
    
    private Edge findByPk(int pk) 
    {
        Edge edge = null;
        
        if (edges.containsKey(pk))
            return edges.get(pk);
        
        try {
            String sql = "select * from " + table + " where lk = " + pk;
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                edge = new Edge();
                edge.init(rs);
            }
            rs.close(); 
            stmt.close();
        } catch (Exception e) {
        }
        return edge;
    }

    private void createIndex(String cols) throws Exception 
    {
        String sql = "CREATE INDEX IF NOT EXISTS " + 
                        cols.replaceAll(",","_") + 
                        "_idx ON "+ table + 
                        "(" + cols + ")";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }
    
    private void createTable() throws Exception 
    {
        //onoffdtcsv - For index 0, Activation is ON, for the rest it toggles 
        String sql = "CREATE TABLE IF NOT EXISTS " + table + " (\n"
                + "	lk integer PRIMARY KEY,\n"
                + "	k0 integer NOT NULL,\n"
                + "	k1 integer NOT NULL,\n"
                + "	k2 integer NOT NULL,\n"
                + "	k3 integer NOT NULL,\n"
                + "	k4 integer NOT NULL,\n"
                + "	dt blob\n"
                + ");";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    //Corresponds to single row in DB
    private class Edge  
    {
        public int lk;
        public int[] key;
        public long[] dt;
        //public int[] from;
        //public int[] to;
        
        public void init(ResultSet rs) throws Exception 
        {
            lk = rs.getInt("lk");
            key = new int[5];
            for (int i=0;i<5;i++)
                key[i] = rs.getInt("k"+i);
            
            Blob blob = rs.getBlob("dt");
            int len = (blob == null)? 0:(int)blob.length();
            int size = (len+7)/8;
            if (size == 0) return;
            dt   = new long[size];
            ByteBuffer buff = ByteBuffer.wrap(blob.getBytes(1,(int)blob.length()));
            for (int i=0;i<dt.length;i++)
            {
                if (buff.hasRemaining()) 
                    dt[i] = Utils.makelong(buff.getInt(),buff.getInt());
            }
        }
        
        //pattern match function 
        public boolean matchKey(int[] pattern) {
            for (int i=0;i<pattern.length;i++)
                if (pattern[i] > 0 && pattern[i] != key[i]) 
                {
                    //System.out.println("hit false");
                    return false;
                }
            //System.out.println(pattern[0] + ":compare:" + key[0]);
            return true;
        }
        
        public void addTime(int ifrom, int ito) 
        {
            if (ito < ifrom) return;
            int from = 0, cnt = (dt == null)? 1:1+dt.length;
            long[] ndt = new long[cnt];
            for (int i=0;i<cnt-1;i++) ndt[i] = dt[i];
            ndt[cnt-1] = Utils.makelong(ifrom,ito); 
            Arrays.sort(ndt);
            cnt = 0; //Reset to compute # of non-overlapping validity periods
            for (int i=0;i<ndt.length-1;i++) 
            {
                int f0 = Utils.msb(ndt[i]);
                int l0 = Utils.lsb(ndt[i]);
                int f1 = Utils.msb(ndt[i+1]);
                int l1 = Utils.lsb(ndt[i+1]);
                if (from != Math.min(f0,f1)) 
                    cnt++;
                from = Math.min(f0,f1);
                if (f1 <= l1) 
                    ndt[i+1] = ndt[i] = Utils.makelong(from,Math.max(l0,l1));
            }
            //Now all records with same fromdt are duplicates
            //Only use the latest among them has the correct to_dt
            dt = new long[cnt];cnt = 0;
            for (int i=0;i<ndt.length-1;i++) 
                if (Utils.msb(ndt[i]) != Utils.msb(ndt[i+1]))
                    dt[cnt++] = ndt[i];
        }
    }
}