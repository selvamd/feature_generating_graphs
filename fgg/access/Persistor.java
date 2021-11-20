package fgg.access;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.nio.*;
import java.sql.*;
import java.text.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.util.Base64;

import fgg.data.*;
import fgg.utils.*;
import org.bson.*;
import org.bson.types.*;
import org.bson.conversions.*;
import com.mongodb.MongoClient;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;

public class Persistor
{
	public static String qdburl() { return dburl(); }

	public static String dburl() {
		return "metadb";
	}

	public static String filestorepath() {
		return (isLinux())? "":"";
	}

	public static void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(
			new Thread() {
				public void run()
				{
                    EnumGroup.flush();
					for (CBOType type:CBOType.values())
						type.recorder.close();
					for (LinkType type:LinkType.values())
						type.recorder.close();
				}
			});
	}

	public static boolean isLinux() {
		return File.pathSeparator.equals(":");
	}

	public static String loadsql(String file) throws Exception
	{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String aline = null, result = "";
		while ((aline = reader.readLine()) != null)
			result += aline + " ";
		return result;
	}

	public static int addDate(int dt, int days) {
		Calendar cal = Calendar.getInstance();
		cal.set(dt/10000, ((dt%10000)/100) - 1, dt%100);
		cal.add(Calendar.DATE, days);
		return cal.get(Calendar.YEAR) * 10000 +
					(1+cal.get(Calendar.MONTH)) * 100 +
					cal.get(Calendar.DAY_OF_MONTH);
	}

	public static int toDateInt(String str)
	{
		str = toDateStr(str);
		if (str == null) return 99999999;
		return Integer.parseInt(str.replaceAll("-",""));
	}

	public static String toDateStr(String str)
	{
		if (str == null || str.length() < 10) return str;
		return str.substring(0,10);
	}


	public static String today() throws Exception {
		return (new SimpleDateFormat("yyyy-MM-dd")).format(Calendar.getInstance().getTime());
	}

	public static Connection getConnectionWithRetries(String db) throws Exception
	{
		Connection con = null;
		while (true)
		{
			if (con != null) return con;
			try {
                String path = filestorepath() + "dbstore/"+db+".db";
                return DriverManager.getConnection("jdbc:sqlite:" + path);
			}
			catch (Exception e) {
				e.printStackTrace();
				con = null;
			}
		}
	}

	public static Map<String,String> lookup(String dburl, String sql, int keys) throws Exception
	{
		Map<String,String> result = new HashMap<String,String>();
        Connection con = getConnectionWithRetries(dburl);
        Statement  stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		if (keys >= rsmd.getColumnCount())
			return result;
		while (rs.next())
		{
			String key = null, val = null;
			for (int i=0; i < rsmd.getColumnCount(); i++)
			{
				String v = rs.getString(i+1);
				if (i >= keys)
					val = (val == null)? v:val + "," + v;
				else
					key = (key == null)? v:key + "," + v;
			}
			result.put(key,val);
		}
        rs.close();stmt.close();con.close();
		return result;
	}

	public static Set<String> keyset(String dburl, String sql) throws Exception
	{
		Set<String> result = new HashSet<String>();
        Connection con = getConnectionWithRetries(dburl);
        Statement  stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		int cols = rsmd.getColumnCount();
		while (rs.next())
		{
			String val = rs.getString(1);
			for (int i=1;i<cols;i++)
			 val += "," + rs.getString(i+1);
			result.add(val);
		}
        rs.close();stmt.close();con.close();
		return result;
	}

	public static String getSingleVal(String dburl, String sql) throws Exception
	{
        Connection con = getConnectionWithRetries(dburl);
        Statement  stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        String v = (rs.next())? rs.getString(1):"";
        rs.close();stmt.close();con.close();
		return v;
	}

    public static Map<String,String> dbFields(String dburl, String sql, boolean enumonly) throws Exception
	{
		Map<String,String> result = new HashMap<String,String>();
        Connection con = getConnectionWithRetries(dburl);
        Statement  stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		int numberOfColumns = rsmd.getColumnCount();
		for (int i=1;i <= numberOfColumns;i++)
		{
			if (enumonly)
			{
				if (rsmd.getColumnTypeName(i).indexOf("char") == -1) continue;
				if (rsmd.getPrecision(i) <= 4) continue;
				if (rsmd.getColumnName(i).indexOf("-") >= 0) continue;
			}

			String value = "";
			String type = rsmd.getColumnTypeName(i);
			if (type.indexOf("char") != -1)
				value = "STRING," + rsmd.getPrecision(i);
			else if (type.indexOf("date") != -1)
				value = "DATE,4";
			else if (type.indexOf("decimal") != -1 || type.indexOf("numeric") != -1)
				value = "DOUBLE," + rsmd.getScale(i);
			else
				value = "INT,4";

			result.put(rsmd.getColumnName(i), value);
		}
		return result;
	}

    public static String query2ddl(String dburl, String sql, String tablename, Set<String> groups)
		throws Exception
	{
        Connection con = getConnectionWithRetries(dburl);
        Statement  stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		int numberOfColumns = rsmd.getColumnCount();
		String ddl = "create table " + tablename + " (\n recordid int, \nfrom_dt int, \nto_dt int,\n";
		for (int i=1;i <= numberOfColumns;i++)
		{
			String type = rsmd.getColumnTypeName(i);
			//rsmd.getColumnClassName(i)
			if (type.indexOf("char") != -1)
			{
				if (type.startsWith("nchar")) type = "char";
				if (type.startsWith("nvarchar")) type = "varchar";
				if (groups != null && groups.contains(rsmd.getColumnName(i)))
					ddl += rsmd.getColumnName(i) + " int,\n";
				else
					ddl += rsmd.getColumnName(i) + " " + type + "(" + rsmd.getPrecision(i) + "),\n";
			}
			else if (type.indexOf("decimal") != -1)
			{
				ddl += rsmd.getColumnName(i) + " " + type + "(" + rsmd.getPrecision(i) + "," + rsmd.getScale(i) + "),\n";
			}
			else
				ddl += rsmd.getColumnName(i) + " " + type + "," + "\n";
		}
		ddl += " PRIMARY KEY (recordid,from_dt)\n" + ")\n";
		return ddl;
	}

	public static int maxkey(String table) throws Exception
	{
		String val = getSingleVal(dburl(), "select max(recordid) from " + table);
		return (val == null)? 0:Integer.parseInt(val);
	}

	//Reads a config record for an attribute
	public static String readConf(String[] rec, String field)
	{
		field += "=";
		for (String str:rec)
			if (str.startsWith(field))
				return str.substring(field.length());
		return null;
	}

	public static Map<String, String[]> loadConf(String filename, String entityname) throws Exception
	{
		HashMap<String,String[]> data = new LinkedHashMap<String,String[]>();
		try
		{
		  BufferedReader reader = new BufferedReader(new java.io.FileReader(filename));
		  String str1 = null;
		  while ((str1 = reader.readLine()) != null)
		  {
			if (str1.startsWith("entity=" + entityname))
			{
			  String[] arrayOfString = str1.split(",");
			  if (arrayOfString[1].startsWith("id="))
			  {
				String str2 = arrayOfString[1].split("=")[1];
				data.put(str2, arrayOfString);
			  }
			}
		  }
		}
		catch (Exception ex) {
			ex.printStackTrace();
		  data.clear();
		}
		return data;
	  }

    public interface QryProcessor { public boolean process(ResultSet rs) throws Exception; };

	//False - reading single row failed. exception - cannot proceed
	private static boolean processSingleRow(QryProcessor processor, ResultSet rs) throws Exception
	{
		for (int i=0; i < 3; i++)
		{
			try {
				processor.process(rs);
				return true;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		return false;
	}

    public static void executeQuery(String dburl, String sql, QryProcessor processor) throws Exception
    {
		try
		{
			Connection con = getConnectionWithRetries(dburl);
			Statement  stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next())
				processSingleRow(processor,rs);
			rs.close();stmt.close();con.close();
		}
		catch (Exception e)
		{
			System.out.println("DBSQL :" + sql);
			System.out.println("ERROR :" + e.getMessage());
			throw e;
		}
    }

	public static String multikey(ResultSet rs, String[] keys) throws Exception
	{
		String key = null;
		for (String kfld:keys)
		{
			String val = (rs.getString(kfld)==null)? "":rs.getString(kfld).trim();
            val = val.replaceAll(","," ");
			key = (key == null)? val:key+","+val;
		}
		return key;
	}

	public static boolean executeUpdate(String dburl, String sql) throws Exception
	{
		try {
			Connection con = getConnectionWithRetries(dburl);
			Statement  stmt = con.createStatement();
			stmt.executeUpdate(sql);
			stmt.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	//Converts a query field to database table column name
	public static String toTableColumn(String name)
	{
		name = name.replaceAll("-","_");
		name = name.replaceAll(" ","_");
		String[] arr = name.split("_");
		name = "";
		for (String s:arr)
			if (s.length() > 0)
				name = name + "_" + s;
		return name.toLowerCase().substring(1);
	}

	public static Map<Integer,String> loadEnums(int val) throws Exception
	{
		Map<Integer,String> result = new HashMap<Integer,String>();
		executeQuery(dburl(), "select * from cbo_dict where recordid > " + val , new QryProcessor() {
			public boolean process(ResultSet rs) throws Exception
			{
				int key = rs.getInt("recordid");
				String val = rs.getString("lookup_name").trim() + "~" + rs.getString("lookup_value").trim();
				result.put(key, val);
				return true;
			}
		});
		return result;
	}

	public static int insertEnum(Map<Integer,String> enums, int id)
	{
		try
		{
            int maxid = id;
            String sql = "INSERT or replace INTO cbo_dict (recordid,lookup_name,lookup_value) VALUES(?,?,?)";
			Connection conn = getConnectionWithRetries(dburl());
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            for (Map.Entry<Integer,String> ent:enums.entrySet())
            {
                maxid = Math.max(id,maxid);
                if (ent.getKey() <= id) continue;
                pstmt.setInt(1,ent.getKey());
                String[] v = ent.getValue().split("~");
                pstmt.setString(2,v[0]);
                if (v.length == 1 || v[1].trim().length() == 0)
                    pstmt.setString(3,"NULL");
                else pstmt.setString(3,v[1]);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            conn.commit();
            pstmt.close();
            conn.close();
            Logger.log(Logger.ENUM_GROUP,"Enum values successfully flushed");
            return maxid;
		}
		catch (Exception e)
		{
            e.printStackTrace();
		}
        return id;
	}

	//Expects "from" and "to" to represent a range [from -> to] of keys to filter and include
	//Make "from" or "to" negative to indicate exclusiveness at the boundaries
	//Make "from" or "to" 0 to indicate open boundary
	//For exact match, make from and to be the same positive value
	public static Set<Integer> findLinkByObjKey(LinkType type, int[] from, int[] to, Set<Integer> out)
	{
		out.clear();
		try {
			String filter = "";
			if (from != null)
			for (int i = 0;i < from.length; i++)
			{
				if (from[i] > 0)
					filter += " AND object_key" + i + " >= " + from[i];
				else if (from[i] < 0)
					filter += " AND object_key" + i + " > " + (-1 * from[i]);
			}
			if (to != null)
			for (int i = 0;i < to.length; i++)
			{
				if (to[i] > 0)
					filter += " AND object_key" + i + " <= " + to[i];
				else if (to[i] < 0)
					filter += " AND object_key" + i + " < " + (-1 * to[i]);
			}
			executeQuery(dburl(),
				"select distinct link_key from dbo_link WHERE edge_key = " + type.ordinal() + filter,
				new QryProcessor() {
					public boolean process(ResultSet rs) throws Exception
					{
						out.add(rs.getInt("link_key"));
						return true;
					}
				});
		} catch (Exception e) {
		}
		return out;
	}
}
