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
 
//Assume sharding is done only by obj key for now. 
//In otherwords all attrs of single object is in same db
public class NodeDataDB extends Persistor 
{
    private Connection conn; 
    private CBOType type; 
    private String table; 
    private int shardInst;
    private int shardCount;
    private Map<Long,NodeData> nodevalues;

    public NodeDataDB(CBOType type, int inst, int count) {
        this.type = type;
        this.table = "nd_" + inst + "_" + type.toString().toLowerCase();
        this.nodevalues = new HashMap<Long,NodeData>();
        this.shardInst = inst;
        this.shardCount = count;
        try {
            conn = getConnectionWithRetries(table);
            createTable();
            createIndex("obj");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int batchSize() { return nodevalues.size(); }

	public  Map<Integer,Field> readRow(int obj, Map<Integer,Field> out) {
        if (obj % shardCount != shardInst) return out;
        out.clear();
        String sql = "select * from " + table + " where obj = " + obj;
        synchronized (this) 
        {
            read(sql,out,true);
            for (NodeData d:nodevalues.values())
                if (d.obj == obj)
                    out.put(d.attr,d.fld);
        }
        //System.out.println("----" + obj + "-----" + out.get(216));
        return out;
    }

	public Map<Integer,Field> readCol(int attr, Map<Integer,Field> out) 
    {
        String sql = "select * from " + table + " where pk > " + 
                     Utils.makelong(attr,0) + " and pk < " + Utils.makelong(attr+1,0);
                     
        synchronized (this) 
        {
            read(sql,out,false);
            for (NodeData d:nodevalues.values())
                if (d.attr == attr)
                    out.put(d.obj,d.fld);
        }
        return out;
    }

    public void upsert(int obj, int attr, int dt, String val) 
    {
        if (obj % shardCount != shardInst) return;
        Logger.log(Logger.SET_NODE_DATA, "Upsert("+attr+","+obj+","+dt+","+val+")");
        long pk = Utils.makelong(attr,obj);
        
        synchronized (this) 
        {
            NodeData node = nodevalues.get(pk);
            if (node == null) 
                node = findByPk(pk);
            
            if (node == null) 
            {
                node = new NodeData();
                node.obj = obj;
                node.attr = attr;
                node.fld = new Field(FieldMeta.lookup(attr));
            }            
            node.fld.seto(dt,val);
            nodevalues.put(pk,node);
            Logger.log(Logger.SET_NODE_DATA, "nodevalue flushed="+node.fld);
            if (nodevalues.size() > 10000) 
            {
                try {
                    flush();
                    nodevalues.clear();
                } catch (Exception e) {
                }
            }
        }
    }
    
    public synchronized void upsert(int obj, Field f) 
    {
        if (obj % shardCount != shardInst) return;
        int attr = f.index(); 
        long pk = Utils.makelong(attr,obj);

        Logger.log(Logger.SET_NODE_DATA, "Upsert2("+obj+","+f+")");
        
        synchronized (this) 
        {
            NodeData node = nodevalues.get(pk);
            if (node == null) 
            {
                node = new NodeData();
                node.obj = obj;
                node.attr = attr;
                node.fld = f;
            } else {
                for (int dt=f.mindt();dt < 99999999; dt = f.nextScdDate(dt))
                    node.fld.seto(dt,f.geto(dt));
            }
            nodevalues.put(pk,node);
            Logger.log(Logger.SET_NODE_DATA, "nodevalue flushed="+node.fld);
            if (nodevalues.size() > 50000) 
            {
                try {
                    flush();
                    nodevalues.clear();
                } catch (Exception e) {
                }
            }
        }
    }

    long count = 0;
    public synchronized void flush() throws Exception 
    {
        System.out.println("Flush called for nodedata " + table + " size = " + nodevalues.size());
        String sql = "INSERT or replace INTO " + table + " VALUES(?,?,?,?)";
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (NodeData node:nodevalues.values()) 
        {
            pstmt.setLong(1,Utils.makelong(node.attr,node.obj));
            pstmt.setInt(2,node.obj);
            pstmt.setInt(3,node.attr);
            ByteBuffer buff = ByteBuffer.allocate(node.fld.length());
            Logger.log(Logger.SET_NODE_DATA, "NodeData write [obj="+node.obj+",attr="+node.attr+"],Buffer=" + node.fld);
            node.fld.serialize(buff);
            pstmt.setBytes(4,buff.array());
            pstmt.addBatch();
            if (count++ % 100000 == 0)
                System.out.println("Added " + count + " records to " + this.table);
        }
        pstmt.executeBatch();
        conn.commit();
        pstmt.close();
        nodevalues.clear();
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
                Logger.log(Logger.GET_NODE_DATA,
                    "NodeData read ["+rs.getInt("obj")+","+rs.getInt("attr")+"]=" + fld);
                if (row) out.put(rs.getInt("attr"),fld);
                else out.put(rs.getInt("obj"),fld);
                //if (table.equals("nd_0_customer"))
                //System.out.println(rs.getInt("obj") + ":"+rs.getInt("attr") + ":"+fld);
            }
            rs.close(); 
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out;
	}


    private NodeData findByPk(long pk) 
    {
        NodeData node = null;
        try {
            String sql = "select * from " + table + " where pk = " + pk;
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            while (rs.next()) {
                node = new NodeData();
                node.init(rs);
            }
            rs.close(); 
            stmt.close();
        } catch (Exception e) {
        }
        return node;
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
                + "	pk integer primary key,\n" //pk = attr+obj
                + "	obj int NOT NULL,\n"
                + "	attr int NOT NULL,\n"
                + " val blob\n"
                + ");";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    private class NodeData  
    {
        public int obj;
        public int attr;
        public Field fld;
        
        public void init(ResultSet rs) throws Exception 
        {
            obj  = rs.getInt("obj");
            attr = rs.getInt("attr");
            fld = new Field(FieldMeta.lookup(attr));
            Blob blob = rs.getBlob("val");
            if (blob == null || blob.length() <= 0) return;
            ByteBuffer buff = ByteBuffer.wrap(blob.getBytes(1,(int)blob.length()));
            fld.deserialize(buff);
        }
    }
}