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
 
//Assume sharding is done only by link/link key for now. 
//In otherwords attrs of single linkect/link is in same db
public class EdgeDataDB extends Persistor {

    private Connection conn; 
    private LinkType type; 
    private String table; 
    private int shardInst;
    private int shardCount;
    private Map<Long,EdgeData> edgevalues;

    public EdgeDataDB(LinkType type, int inst, int count) {
        this.type = type;
        this.table = "ld_" + inst + "_" + type.toString().toLowerCase();
        this.edgevalues = new HashMap<Long,EdgeData>();
        this.shardInst = inst;
        this.shardCount = count;
        try {
            conn = getConnectionWithRetries(table);
            createTable();
            createIndex("link");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int batchSize() { return edgevalues.size(); }

	public  Map<Integer,Field> readRow(int link, Map<Integer,Field> out) {
        if (link % shardCount != shardInst) return out;
        out.clear();
        String sql = "select * from " + table + " where link = " + link;
        synchronized (this) 
        {
            read(sql,out,true);
            for (EdgeData d:edgevalues.values())
                if (d.link == link)
                    out.put(d.attr,d.fld);
        }
        return out;
    }

	public Map<Integer,Field> readCol(int attr, Map<Integer,Field> out) {
        String sql = "select * from " + table + " where pk > " + Utils.makelong(attr,0) +
                     " and pk < " + Utils.makelong(attr+1,0);
        synchronized (this) 
        {
            read(sql,out,false);
            for (EdgeData d:edgevalues.values())
                if (d.attr == attr)
                    out.put(d.link,d.fld);
        }
        return out;
    }

    public void upsert(int link, int attr, int dt, String val) 
    {
        if (link % shardCount != shardInst) return;
        //System.out.println(attr+","+link+","+dt+","+val);
        long pk = Utils.makelong(attr,link);
        synchronized (this) 
        {
            EdgeData edge = edgevalues.get(pk);
            if (edge == null) 
                edge = findByPk(pk);
            
            if (edge == null) 
            {
                edge = new EdgeData();
                edge.link = link;
                edge.attr = attr;
                edge.fld = new Field(FieldMeta.lookup(attr));
            }            
            edge.fld.seto(dt,val);
            edgevalues.put(pk,edge);
            if (edgevalues.size() > 500) 
            {
                try {
                    flush();
                    edgevalues.clear();
                } catch (Exception e) {
                }
            }
        }
    }
    
    public synchronized void upsert(int link, Field f) 
    {
        if (link % shardCount != shardInst) return;
        int attr = f.index(); 
        long pk = Utils.makelong(attr,link);
        
        synchronized (this) 
        {
            EdgeData edge = edgevalues.get(pk);
            if (edge == null) 
            {
                edge = new EdgeData();
                edge.link = link;
                edge.attr = attr;
                edge.fld = f;
            }            
            edgevalues.put(pk,edge);
            if (edgevalues.size() > 500) 
            {
                try {
                    flush();
                    edgevalues.clear();
                } catch (Exception e) {
                }
            }
        }
    }

    public synchronized void flush() throws Exception 
    {
        String sql = "INSERT or replace INTO " + table + " VALUES(?,?,?,?)";
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (EdgeData edge:edgevalues.values()) 
        {
            pstmt.setLong(1,Utils.makelong(edge.attr,edge.link));
            pstmt.setInt(2,edge.link);
            pstmt.setInt(3,edge.attr);
            ByteBuffer buff = ByteBuffer.allocate(edge.fld.length());
            edge.fld.serialize(buff);
            pstmt.setBytes(4,buff.array());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        conn.commit();
        pstmt.close();
        edgevalues.clear();
    }

    /////////////////////////////////////// Private methods /////////////////////////////
	private Map<Integer,Field> read(String sql, Map<Integer,Field> out, boolean row)
	{
        try {
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                Field fld = new Field(FieldMeta.lookup(rs.getInt("attr")));
                byte[] bytes = rs.getBytes("val");
                if (bytes == null) continue;
                ByteBuffer buff = ByteBuffer.wrap(rs.getBytes("val"));
                fld.deserialize(buff);
                if (row) out.put(rs.getInt("attr"),fld);
                else out.put(rs.getInt("link"),fld);
                //if (table.equals("nd_0_customer"))
                //System.out.println(rs.getInt("link") + ":"+rs.getInt("attr") + ":"+fld);
            }
            rs.close(); 
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out;
	}


    private EdgeData findByPk(long pk) 
    {
        EdgeData edge = null;
        try {
            String sql = "select * from " + table + " where pk = " + pk;
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                edge = new EdgeData();
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
        String sql = "CREATE INDEX  IF NOT EXISTS " + 
                      cols.replaceAll(",","_")+"_idx ON " + 
                      table + "(" + cols + ")";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }
    
    private void createTable() throws Exception 
    {
        //onoffdtcsv - For index 0, Activation is ON, for the rest it toggles 
        String sql = "CREATE TABLE IF NOT EXISTS " + table + " (\n"
                + "	pk integer primary key,\n" //pk = attr+link
                + "	link int NOT NULL,\n"
                + "	attr int NOT NULL,\n"
                + " val blob\n"
                + ");";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    private class EdgeData  
    {
        public int link;
        public int attr;
        public Field fld;
        
        public void init(ResultSet rs) throws Exception 
        {
            link  = rs.getInt("link");
            attr = rs.getInt("attr");
            fld = new Field(FieldMeta.lookup(attr));
            byte[] bytes = rs.getBytes("val");
            if (bytes == null) return;
            ByteBuffer buff = ByteBuffer.wrap(bytes);
            fld.deserialize(buff);
        }
    }
}