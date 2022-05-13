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

//1) Each object has unique int objkey (altnum=0) and strkeys altnum 1..n
//2) Altnum represents an alternate external sourceid that supplies a strkey
//3) strkey collision can happen across sources but not within same source
//4) No source needs to have all the objects. All objs is union of all srcs
//5) strkey can be composite and map to 1 or more attrs in NodeData which in
//   nodekeydb gets represented as fld1/fld2/fld3
public class NodeKeyDB extends Persistor
{
    private Connection conn;
    private CBOType type;
    private String table;
    private int maxkey = 0;
    private int maxalt = 0;

    //Transient buffer for unpersisted keys
    private Map<String,NodeKey> nodekeys;  //str+'/'+alt->obj

    public NodeKeyDB(CBOType type)
    {
        this.type  = type;
        this.table = "nk_"+type.toString().toLowerCase();
        this.nodekeys  = new HashMap<String,NodeKey>();
        try {
            conn = getConnectionWithRetries(table);
            createTable();
            createIndex("hk");
            maxkey();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int nextkey()    { return maxkey+1;        }
    public int batchSize()  { return nodekeys.size(); }

    //Read internal pk
	public synchronized int read(String key, int alt)
    {
        NodeKey nk = nodekeys.get(key+"/"+alt);
        if (nk == null) nk = findByPk(0,key,alt);
        return (nk == null)? null:nk.pk;
    }

    //read external src key
	public synchronized String read(int key,int alt) {
        NodeKey nk = null;
        for (NodeKey n:nodekeys.values())
            if (n.pk == key && n.alt == alt)
                nk = n;
        if (nk == null) nk = findByPk(key,null,alt);
        return (nk == null)? null:nk.str;
    }

	public synchronized Map<Integer,Integer> readAll(String key,
        Map<Integer,Integer> out)
    {
        out.clear();
        for (NodeKey n:nodekeys.values())
            if (key == null || key.length() == 0)
                out.put(n.pk,0);
            else if (n.str.equals(key))
                out.put(n.pk,n.alt);
        findAll(key,out);
        return out;
    }

    public synchronized void upsert(int pk, int alt, String str)
    {
        NodeKey nk = null;
        if (pk <= 0) {
            nk = new NodeKey();
            pk = nk.pk = nextkey();
            nk.str = ""+nk.pk;
            nk.hk  = nk.str.hashCode();
            nk.alt = 0;
            nodekeys.put(nk.str+"/0",nk);
            maxkey = Math.max(maxkey,nk.pk);
            maxalt = Math.max(maxalt,nk.alt);
        }

        for (NodeKey n:nodekeys.values())
            if (n.pk == pk && n.alt == alt)
                nk = n;

        if (nk == null) nk = findByPk(pk, null, alt);
        if (nk == null) nk = new NodeKey();
        nk.hk  = str.hashCode();
        nk.str = str;
        nk.alt = alt;
        nodekeys.put(nk.str+"/"+nk.alt,nk);
        if (nodekeys.size() > 10000) {
            try {
                flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    long count = 0;
    public synchronized void flush() throws Exception
    {
        System.out.println("Flush called for nodekey " + table + " size = " + nodekeys.size());
        String sql = "INSERT or replace INTO " + table + " VALUES(?,?,?,?)";
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (NodeKey nk:nodekeys.values())
        {
            pstmt.setInt(1,nk.pk);
            pstmt.setInt(2,nk.alt);
            pstmt.setString(3,nk.str);
            pstmt.setInt(4,nk.hk);
            pstmt.addBatch();
            if (count++ % 100000 == 0)
                System.out.println("Added " + count + " records to " + this.table);
        }
        pstmt.executeBatch();
        conn.commit();
        pstmt.close();
        nodekeys.clear();
    }

    /////////////////////////////////////// Private methods /////////////////////////////
     private void maxkey() throws Exception {
        int key = 100;
        String sql = "select max(pk), max(alt) from " + table;
        Statement stmt  = conn.createStatement();
        ResultSet rs    = stmt.executeQuery(sql);
        if (rs.next()) {
            maxkey = rs.getInt(1);
            maxalt = rs.getInt(2);
        }
        stmt.close();
        rs.close();
    }

    private NodeKey findByPk(int pk, String key, int alt)
    {
        NodeKey nodekey = null;
        String sql = null;
        try {
            if (pk > 0)
                sql = "select * from " + table + " where pk = " + pk + " and alt = " + alt;
            else
                sql = "select * from " + table + " where alt = " + alt + " and str = '" + key + "'";
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                nodekey = new NodeKey();
                nodekey.init(rs);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            nodekey = null;
        }
        return nodekey;
    }

    private void findAll(String str, Map<Integer,Integer> out) {
        try {
            String sql = "select pk, alt, str from " + table;
            if (str != null && str.length() > 0)
                sql += " where hk = " + str.hashCode();
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                if (str != null && str.length() > 0)
                    if (!rs.getString("str").equals(str))
                        continue;
                out.put(rs.getInt("pk"),rs.getInt("alt"));
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
        }
    }

    private void createIndex(String cols) throws Exception
    {
        String sql = "CREATE INDEX IF NOT EXISTS "+
                    cols.replaceAll(",","_") + "_idx ON " +
                    table+"("+cols+")";

        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    private void createTable() throws Exception
    {
        //pk objkey, alt - altnum, hk - hash, str -> value
        String sql = "CREATE TABLE IF NOT EXISTS " + table + " (\n"
                + "	pk integer,\n"
                + "	alt integer,\n"
                + "	str text NOT NULL,\n"
                + "	hk integer,\n"
                + " primary key (pk,alt)\n"
                + ");";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    private class NodeKey
    {
        public int pk;
        public int alt;
        public int hk;
        public String str;

        public String toString() {
            return pk + "," + alt + "," + str + "," + hk;
        }

        public void init(ResultSet rs) throws Exception
        {
            pk = rs.getInt("pk");
            alt = rs.getInt("alt");
            hk = rs.getInt("hk");
            str = rs.getString("str");
        }
    }
}
