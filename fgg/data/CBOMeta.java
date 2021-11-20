package fgg.data;

import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.*;
import fgg.utils.*;
import fgg.access.*;
	
public class CBOMeta
{
	public String  name;
	public String  jclass;
	public String  label;
	public String  dbtype;
	public int     dbsize;
	public int     dbscale;
	public int 	   index;
	public boolean isCboKey;
	public String  table;
	public boolean isGroup;

  public String toString() {
		  return name + "," + dbtype + "," + dbsize;
	}
	public int bytesize() 
	{
		if (dbtype.indexOf("char") != -1)
			return dbsize;
		else if (dbtype.indexOf("date") != -1 || dbtype.equals("int"))
			return 4;
		else 
			return 8;
	}
	
	public boolean verify(String val)
	{
		if (dbtype.indexOf("binary") != -1)
			return true;
		
		if (dbtype.indexOf("char") != -1)
		{
			if (val == null) return true;
			return (val.length() <= dbsize);
		}
		else if (dbtype.indexOf("date") != -1)
		{
			if (val == null) return true;
			val = val.replaceAll("-","");
			if (val.length() > 8) 
				val = val.substring(0,8);
			try { 
				int dt = Integer.parseInt(val);
				return (dt > 19000101 && dt < 21000101);
			} catch (Exception e) {
				return false;
			}
		}
		else 
		{
			if (val == null) return true;
			return val.matches("-?\\d+(\\.\\d+)?");
		}
	}
	
	public boolean compare(String val1, String val2)
	{
		if (val1 == null && val2 == null) 
			return true;
		
		if (val1 == null || val2 == null)
			return false;

		if (dbtype.indexOf("binary") != -1)
			return true;
		
		if (dbtype.indexOf("char") != -1) 
			return val1.trim().equals(val2.trim());
		
		if (dbtype.indexOf("date") != -1)
		{
			val1 = val1.replaceAll("-","");
			val2 = val2.replaceAll("-","");
			if (val1.length() > 8) val1 = val1.substring(0,8);
			if (val2.length() > 8) val2 = val2.substring(0,8);
			return val1.trim().equals(val2.trim());
		}
		
		double dval1 = Double.parseDouble(val1.trim());
		double dval2 = Double.parseDouble(val2.trim());
		
		if (dval1 > dval2) 
			return (dval1 - dval2 < 0.000001);
		else 
			return (dval2 - dval1 < 0.000001);
	}
	
	public void init(ResultSetMetaData rsmd, int index, String[] keys, String[] groups, String table)
		throws Exception
	{
		this.index = index;
		name    = rsmd.getColumnName(index);
		jclass  = rsmd.getColumnClassName(index);
		label   = rsmd.getColumnLabel(index);
		dbtype  = rsmd.getColumnTypeName(index);
		dbtype  = dbtype.toLowerCase();
		dbsize  = rsmd.getPrecision(index);
		dbscale = rsmd.getScale(index);
		this.table = table;
		isCboKey = false;
		for (String s:keys)
			if (s.equalsIgnoreCase(name))
				isCboKey = true;
		isGroup = false;
		for (String s:groups)
			if (s.equalsIgnoreCase(name))
				isGroup = true;
	}

		public static CBOMeta[] include(CBOMeta[] fields, String keys)
		{
			if (keys == null) return null;
			List<CBOMeta> metas = new ArrayList<CBOMeta>();
			String k[] = keys.split(",");
			for (String name:k)
				for (CBOMeta m: fields)
				if (m.name.equalsIgnoreCase(name))
					metas.add(m);
			//All keys donot match, return null	
			if (metas.size() != k.length)
				return null;
			return (CBOMeta[])metas.toArray(new CBOMeta[metas.size()]);
		}

		public static CBOMeta[] exclude(CBOMeta[] fields, String keys)
		{
			if (keys == null) return null;
			List<CBOMeta> metas = new ArrayList<CBOMeta>();
			keys = "," + keys.toUpperCase() + ",";
			for (CBOMeta m: fields)
			{
				if (keys.indexOf("," + m.name.toUpperCase() + ",") >= 0) 
					continue;
				metas.add(m);
			}
			if (metas.size() == 0) return null;
			return (CBOMeta[])metas.toArray(new CBOMeta[metas.size()]);
		}
		
		public static CBOMeta[] makekey(CBOMeta[] fields, String keys)
		{
			List<CBOMeta> metas = new ArrayList<CBOMeta>();
			if (keys == null || keys.trim().length() == 0)
			{
				for (CBOMeta m: fields)
					if (m.isCboKey)
						metas.add(m);
				return (CBOMeta[])metas.toArray(new CBOMeta[metas.size()]);
			}
			return include(fields,keys);
		}
		
		public static CBOMeta[] buildMeta(String tablename) throws Exception { 
			return CBOPersistor.buildMeta(tablename, null, null); 
		}
		
		public static CBOMeta[] buildMeta(String tablename, String[] keys) throws Exception { 
			return CBOPersistor.buildMeta(tablename, keys, null); 
		}
		
		public static CBOMeta[] buildMeta(String tablename, String[] keys, String[] groups) throws Exception {
			return CBOPersistor.buildMeta(tablename, keys, groups); 
		}
}
