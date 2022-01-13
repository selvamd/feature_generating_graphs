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

public class NodeKeyDB extends Persistor
{
    private Connection conn;
    private CBOType type;
    private String table;
    private int maxkey = 0;

    //Donot expect overlap between alternate external keys
    private Map<String,NodeKey> nodekeys;  //str->obj
    private Map<String,NodeKey> inodekeys; //objkey+'/'+alt->obj

    public NodeKeyDB(CBOType type)
    {
        this.type  = type;
        this.table = "nk_"+type.toString().toLowerCase();
        this.nodekeys  = new HashMap<String,NodeKey>();
        this.inodekeys = new HashMap<String,NodeKey>();
        try {
            conn = getConnectionWithRetries(table);
            createTable();
            createIndex("hk");
            maxkey = maxkey();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int nextkey()    { return maxkey+1;        }
    public int batchSize()  { return nodekeys.size(); }

	public synchronized String read(int key, int alt) {
        NodeKey nk = inodekeys.get(key+"/"+alt);
        if (nk == null) nk = findByPk(key,alt);
        return (nk == null)? null:nk.str;
    }

	public synchronized Map<Integer,Integer> readAll(String match, Map<Integer,Integer> out) {
        out.clear();
        if (nodekeys.containsKey(match))
            out.put(nodekeys.get(match).pk, nodekeys.get(match).alt);
        else if (match == null || match.length() == 0)
            nodekeys.forEach((k,v)->out.put(v.pk,v.alt));
        findAll(match,out);
        return out;
    }

	public synchronized int read(String key) {
        NodeKey nk = nodekeys.get(key);
        if (nk == null) nk = findByPk(key);
        return (nk == null)? -1:nk.pk;
    }

    public synchronized void upsert(int pk, int alt, String str, String oldkey)
    {
        //new key already exists
        int pk1 = read(str);
        if (pk1 > 0) return;

        //old key exists
        NodeKey nodekey = null;
        if (pk > 0)
            nodekey = (inodekeys.containsKey(pk+"/"+alt))?
                        inodekeys.get(pk+"/"+alt):findByPk(pk,alt);
        else if (oldkey != null)
            nodekey = (nodekeys.containsKey(oldkey))?
                        nodekeys.get(oldkey):findByPk(oldkey);

        if (nodekey == null)
        {
            nodekey = new NodeKey();
            nodekey.pk = (pk <= 0)? nextkey():pk;
        }
        nodekey.hk  = str.hashCode();
        nodekey.str = str;
        nodekey.alt = alt;

        nodekeys.remove(oldkey);
        maxkey = Math.max(maxkey,nodekey.pk);
        nodekeys.put(str,nodekey);
        inodekeys.put(nodekey.pk+"/"+nodekey.alt,nodekey);
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
        System.out.println("Flush called for nodekey " + table + " size = " + inodekeys.size());
        String sql = "INSERT or replace INTO " + table + " VALUES(?,?,?,?)";
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (NodeKey nk:inodekeys.values())
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
        inodekeys.clear();
    }

    /////////////////////////////////////// Private methods /////////////////////////////
     private int maxkey() throws Exception {
        int key = 100;
        String sql = "select max(pk) from " + table;
        Statement stmt  = conn.createStatement();
        ResultSet rs    = stmt.executeQuery(sql);
        if (rs.next()) key = rs.getInt(1);
        stmt.close();
        rs.close();
        return key;
    }

    private NodeKey findByPk(int pk, int alt)
    {
        NodeKey nodekey = null;
        try {
            String sql = "select * from " + table + " where pk = " + pk + " and alt = " + alt;
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

    private NodeKey findByPk(String str)
    {
        NodeKey nodekey = null;
        try {
            String sql = "select * from " + table + " where hk = " + str.hashCode();
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                if (!str.equals(rs.getString("str")))
                    continue;
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
