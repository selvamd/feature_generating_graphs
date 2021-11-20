package fgg.access;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.*;
import java.nio.charset.*;
import java.sql.*;
import fgg.utils.*;

//Cache object for all enum lookups using table cbo_dict as dbstore 
public class EnumGroup 
{
    private static final int ENUM_CACHE_SIZE = 1000;
	private static int loadmaxvalue = 0;
	private static int maxvalue = 0;
	private static Map<String,Integer> namelookup = new HashMap<String,Integer>();
	private static Map<Integer,String> indexlookup = new HashMap<Integer,String>();
	private static Charset charset = Charset.forName("UTF-8");
		
	public static boolean load()
	{
        boolean res = loadById(0);
        System.out.println("Loaded enum group records " + namelookup.size());
		return res;
	}

	private static boolean loadById(int val) 
	{
		try {
			for (Map.Entry<Integer,String> ent: Persistor.loadEnums(val).entrySet())
			{
				namelookup.put(ent.getValue(), ent.getKey());
				indexlookup.put(ent.getKey(), ent.getValue());
				loadmaxvalue = maxvalue = Math.max(maxvalue,ent.getKey());
			}
            Logger.log(Logger.ENUM_GROUP, "Loaded enum group records " + namelookup.size());
			return true;
		} catch (Exception e) {
            e.printStackTrace();
		}
		return false;
	} 
	
	private static synchronized void insert(String nam, String val) 
	{
		val = val.replaceAll("'", "''");
        int id = ++maxvalue;
        namelookup.put(nam+"~"+val, id);
        indexlookup.put(id, nam+"~"+val);
        Logger.log(Logger.ENUM_GROUP, "New value inserted " + nam + "=" + val);
        if (maxvalue - loadmaxvalue < ENUM_CACHE_SIZE) return; 
		loadmaxvalue = Persistor.insertEnum(indexlookup, loadmaxvalue);
        Logger.log(Logger.ENUM_GROUP, "Auto-Flush on enumgroup called");
        //System.out.println("Insert ============> " + nam + ":" + val);
		//loadById(maxvalue);
	}

	public static void flush() {
        Logger.log(Logger.ENUM_GROUP, "Flush on enumgroup called");
		loadmaxvalue = Persistor.insertEnum(indexlookup, loadmaxvalue);
    }
    
	public static int index(String nam, String val)
	{
		if (val == null) return -1;
		val = charset.decode(charset.encode(val)).toString();
		return getindex(nam, val);
	}

	private static int getindex(String nam, String val)
	{
		Integer idx = namelookup.get(nam + "~" + val);
		return (idx == null)? -1:idx.intValue();
	}
	
	public static int add(String nam, String val)
	{
		if (val == null) return -1;
		val = val.trim();
		val = charset.decode(charset.encode(val)).toString();
		int idx = getindex(nam, val);
		if (idx > 0) return idx;
		insert(nam, val);
		return getindex(nam, val);
	}

	public static List<String> values(String name, List<String> res)
	{
		res.clear();
		for (String str:namelookup.keySet())
            if (str.startsWith(name))
                res.add(str.substring(1+str.indexOf("~")));
		return res;
	}
	
	public static String value(int rec)
	{
		String txt = indexlookup.get(rec);
		if (txt != null)
			txt = txt.substring(1+txt.indexOf("~"));
        //System.out.println(rec + "=========>>"+txt);
        if (txt == null || txt.equals("NULL")) txt = "";
		return txt;	
	}
}
