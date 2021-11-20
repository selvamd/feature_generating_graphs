package fgg.data;

import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.*;
import java.math.*;
	
public class CBO
{
	private int recid;
	private int fromdt;
	private int todt;
	private int currdt;
	boolean checked = false;
	
	private CBOMeta[] fields;
	private String[] values;

	public CBO(CBOMeta[] fields)
	{
		this.fields = fields;
		this.values = new String[fields.length];
		checked = false;
	}
	
	public int recid() { return recid; }
	public int fromdt() { return fromdt; }
	public int todt() { return todt; }
	public CBOMeta[] meta() { return fields; }

	public int currdt() { return currdt; }
	public  void setcurrdt(int dt) { currdt = dt; }
	
	public void initQDB_Date(ResultSet rs) throws Exception 
	{
		java.sql.Date dt;
		recid   = rs.getInt("recordid");

		dt = rs.getDate("from_dt");
		fromdt  = Integer.parseInt(dt.toString().replaceAll("-", ""));
		dt = rs.getDate("to_dt");
		todt  	= Integer.parseInt(dt.toString().replaceAll("-", ""));
		for (CBOMeta m:fields)
			values[m.index-1] = rs.getString(m.index);
		checked = false;
	}
	
	//Loads CBO records from QuantDB
	public void initQDB(ResultSet rs) throws Exception 
	{
		recid   = rs.getInt("recordid");
		fromdt  = rs.getInt("from_dt");
		todt    = rs.getInt("to_dt");
		for (CBOMeta m:fields)
			values[m.index-1] = rs.getString(m.index);
		checked = false;
	}

	public boolean verify()
	{
		for (CBOMeta m:fields)
			if (!m.verify(values[m.index-1]))
				return false;
		return true;
	}
	
	public void initCsv(String csv)
	{
		values = csv.split(",");
		recid   = Integer.parseInt(values[0]);
		fromdt  = Integer.parseInt(values[1]);
		todt    = Integer.parseInt(values[2]);
		for (CBOMeta m:fields)
		{
			if (m.dbtype.indexOf("date") != -1)
				if ("99999999".equals(values[m.index-1]))
					values[m.index-1] = null;
		}
		for (int i=0;i < values.length; i++)
			if (values[i].equalsIgnoreCase("null"))
				values[i] = "";
	}
	
	public void initFile(String str) throws Exception 
	{
		String arr[] = str.split(",");
		for (String s: arr)
		{
			String namval[] = s.split("=");

			if (namval.length < 2) continue;
			
			if ("null".equalsIgnoreCase(namval[1]))
				namval[1] = "";
			
			for (CBOMeta m:fields)
			{
				if (!m.name.equals(namval[0])) continue;
				String val = namval[1];
				if ("recordid".equals(namval[0]))
					recid = Integer.parseInt(values[m.index-1] = val);
				else if ("from_dt".equals(namval[0]))
					fromdt = Integer.parseInt(values[m.index-1] = (val==null)? "19000101":val);
				else if ("to_dt".equals(namval[0]))
					todt = Integer.parseInt(values[m.index-1] = (val==null)? "99999999":val);

				values[m.index-1] = namval[1];
				if (m.dbtype.indexOf("date") != -1)
					if ("99999999".equals(values[m.index-1]))
						values[m.index-1] = null;
			}
		}
	}

	public void initNew(ResultSet rs, int recordid, int nowdt) throws Exception 
	{
		initNew(rs, recordid, nowdt, fields);
	}
	
	public void initNew(ResultSet rs, int recordid, int nowdt, CBOMeta[] metas) throws Exception 
	{
		recid   = recordid;
		fromdt  = nowdt;
		todt    = 99999999;

		for (CBOMeta m:fields)
		{
			if (m.name.equals("recordid")) values[m.index-1] = ""+recordid;
			else if (m.name.equals("from_dt")) values[m.index-1] = ""+nowdt;
			else if (m.name.equals("to_dt")) values[m.index-1] = "99999999";
		}
		
		for (CBOMeta m:metas)
		{
			if (m.name.equals("recordid")) continue;
			else if (m.name.equals("from_dt")) continue;
			else if (m.name.equals("to_dt")) continue;
			else values[m.index-1] = rs.getString(m.name);
			if (values[m.index-1] == null) continue;
			values[m.index-1] = values[m.index-1].trim();
			if (m.dbtype.indexOf("date") != -1) 
				values[m.index-1] = values[m.index-1].replaceAll("-","");
			else if (m.dbtype.indexOf("char") != -1 && values[m.index-1].length() > m.dbsize)
				values[m.index-1] = values[m.index-1].substring(0,m.dbsize);
			else if (m.dbtype.indexOf("decimal") != -1)
			{
				String[] str = new BigDecimal(rs.getString(m.name)).toPlainString().split("\\.");
				if (str.length == 2 && str[1].length() > m.dbscale)
					str[1] = str[1].substring(0,m.dbscale);
				values[m.index-1] = str[0];
				if (str.length == 2) values[m.index-1] += "." + str[1];
			}
		}
		checked = false;
	}
	
	public String cbokey() { return cbokey(null); }
	public String cbokey(CBOMeta[] keys)
	{
		boolean usedefault = false;
		String key = null;
		if (keys == null)
		{
			usedefault = true;
			keys = fields;
		}
		for (CBOMeta m:keys)
		{	
			if (usedefault && !m.isCboKey) continue;
			String val = (values[m.index-1] == null)? 
						 "":values[m.index-1].trim();
			key = (key == null)? val:key + "/" + val;
		}
		return key.toUpperCase();
	}

	public boolean setDt(CBOMeta m, int value)
	{
		if (value == 99999999)
			return set(m, null);
		String dt = ""+value;
		return set(m, dt.substring(0,4) + "-" + dt.substring(4,6) + "-" + dt.substring(6)); 
	}
	public boolean set(CBOMeta m, int value) { return set(m,""+value); }
	public boolean set(CBOMeta m, String value)
	{
		if (m.dbtype.indexOf("char") != -1 && value == null)
			values[m.index-1] = null;
		else if (m.dbtype.indexOf("char") != -1 && value.length() > m.dbsize)
			values[m.index-1] = value.substring(0,m.dbsize);
		else if (m.dbtype.indexOf("char") != -1)
			values[m.index-1] = value;
		else if (m.dbtype.indexOf("date") != -1)
			values[m.index-1] = value;
		else if (value != null)
			values[m.index-1] = (value.matches("^[-+]?\\d+(\\.\\d+)?$"))? value:"0";
		else 
			values[m.index-1] = "0";
		
		if ("recordid".equals(m.name)) recid = Integer.parseInt(values[m.index-1]);
		if ("from_dt".equals(m.name)) fromdt = Integer.parseInt(values[m.index-1]);
		if ("to_dt".equals(m.name)) todt = Integer.parseInt(values[m.index-1]);
		
		return true;
	}
	
	public boolean set(String field, String value)
	{
		for (CBOMeta m:fields)
		{
			if (!m.name.equals(field)) continue;
			return set(m,value);
		}	
		return false;
	}

	public String get(CBOMeta m)
	{
		return values[m.index-1];
	}
	
	public String get(String field)
	{
		for (CBOMeta m:fields)
			if (m.name.equals(field))
				return get(m);
		return null;
	}

	//persists new cbo records
	public void persistnew(PreparedStatement st) throws Exception
	{
		for (CBOMeta m:fields)
			st.setString(m.index, values[m.index-1]);
	}
	
	public String toString()
	{
		String result = fields[0].table;
		for (CBOMeta m:fields)
			result += "," + m.name + "=" + values[m.index-1];
		return result;
	}

	public String toCsv()
	{
		String result = null;
		for (CBOMeta m:fields)
		{
			String fld = values[m.index-1];
			if (fld == null || "null".equalsIgnoreCase(fld)) fld = "";
			if (m.dbtype.indexOf("char") != -1) fld = "\"" + fld + "\""; 
			result = (result == null)?  fld: result + "," + fld;
		}
		return result;
	}

	//Methods to check/update cbo objects that have changed
	public boolean checked() { return checked; }
	public void setchg(int nowdt) throws Exception { 
		checked = true;
		todt = nowdt; 
		for (CBOMeta m:fields)
			if ("to_dt".equals(m.name))
				values[m.index-1] = ""+nowdt;
	}

	
	public boolean chkchg(CBO cbo, int nowdt, CBOMeta[] compares) throws Exception
	{
		checked = true;

		//Ignore if it is not next date
		if (nowdt <= fromdt) return false;
		
		if (compares == null) 
			compares = fields;
		
		for (CBOMeta m:compares)
		{
			if ("recordid".equals(m.name) || "from_dt".equals(m.name) || 
				"to_dt".equals(m.name)) continue;
			
			if (m.compare(cbo.get(m),values[m.index-1]))
				continue;
			
			return true;
		}	
		return false;
	}

	public boolean chkchg(ResultSet rs, int nowdt, CBOMeta[] compares) throws Exception
	{
		checked = true;

		//Ignore if it is not next date
		if (nowdt > 0 && nowdt <= fromdt) 
			return false;
			
		if (compares == null) 
			compares = fields;

		for (CBOMeta m:compares)
		{
			if ("recordid".equals(m.name) || "from_dt".equals(m.name) || 
				"to_dt".equals(m.name)) continue;
			
			String val = rs.getString(m.name);
			if (val != null) val = val.trim();
				
			if (m.compare(val,values[m.index-1]))
				continue;
			
			return true;
		}
		return false;
	}

	public static String insertStmtSql(CBOMeta[] fields) throws Exception
	{
		String sql = "insert into " + fields[0].table + "(";
		for (CBOMeta m:fields) sql = (m.index == 1)? sql + m.name: sql + "," + m.name;
		sql += ") values (";
		for (CBOMeta m:fields) sql = (m.index == 1)? sql + "?": sql + ",?";
		sql += ")";
		return sql;
	}

	public void persistUpdateInsert(PreparedStatement stmt, String keys) throws Exception
	{
		int idx = 0;
		for (CBOMeta m:fields)
		{
			if ("recordid".equals(m.name)) continue;
			if ("from_dt".equals(m.name)) continue;
			stmt.setString(++idx, values[m.index-1]);
		}
		for (String m:keys.split(","))
				stmt.setString(++idx, get(m));
		for (CBOMeta m:fields)
				stmt.setString(++idx, values[m.index-1]);
	}
	
	public void persistupdate(PreparedStatement stmt) throws Exception
	{
		int idx = 0;
		for (CBOMeta m:fields)
			stmt.setString(++idx, values[m.index-1]);
		stmt.setInt(++idx, recid);
		stmt.setInt(++idx, fromdt);
	}

	public void persistupdate4scd(PreparedStatement stmt) throws Exception
	{
		stmt.setInt(1, todt);
		stmt.setInt(2, recid);
	}

	//update test set name='john' where id=3012 IF @@ROWCOUNT=0 insert into test(name) values('john');
	public static String updateOrInsertStmtSql(CBOMeta[] fields, String keys) throws Exception
	{
		String sql = "update " + fields[0].table + " set ";
		String str = null;
		for (CBOMeta m:fields) 
		{
			if ("recordid".equals(m.name)) continue;
			if ("from_dt".equals(m.name)) continue;
			str = (str == null)? "" : str + ",";
			str += m.name + " = ? ";
		}
		sql += str + " where ";

		str = null;
		for (String m:keys.split(","))
			str = m + " = ? " + ((str == null)? "" : " and " + str);

		sql += str + " IF @@ROWCOUNT=0 insert into " + fields[0].table + "(";
		str = null;
		for (CBOMeta m:fields) 
		{
			str = (str == null)? "" : str + ",";
			str += m.name;
		}
		sql += str + ") values(";
		str = null;
		for (CBOMeta m:fields) 
		{
			str = (str == null)? "" : str + ",";
			str += "?";
		}
		sql += str + ")";
		return sql;
	}
   
	public static String updateStmtSql(CBOMeta[] fields) throws Exception
	{
		String sql = "update " + fields[0].table + " set ";
		for (CBOMeta m:fields) 
			sql = (m.index == 1)? sql + m.name + " = ? ":
					sql + "," + m.name + " = ? ";
		sql += " where recordid = ? and from_dt = ?";
		return sql;
	}
	
	public static String updateStmtSql4Scd(CBOMeta[] fields) throws Exception
	{
		return "update " + fields[0].table + " set to_dt = ? where recordid = ? and to_dt = 99999999";
	}

	public static String sqlExprForCboKey(CBOMeta[] fields)
	{
		String cbokey = null;
		for (CBOMeta m:fields)
		{
			if (!m.isCboKey) continue;
			String val = "cast(" + m.name + " as varchar)";
			cbokey = (cbokey == null)? val: cbokey + " + '/' + " + val;
		}
		return "(" + cbokey + ")";
	}
	
	public static String queryActiveCBO(CBOMeta[] fields, String fromcbo, String tocbo, String basesql, String where)
	{
		String cbokey = sqlExprForCboKey(fields);
		if (where != null) where = " where " + where;
		if (fromcbo != null && fromcbo.length() > 0)
		{
			where = (where == null)? " where ": where + " and ";
			where += cbokey + " >= '" + fromcbo + "'";
		}

		if (tocbo != null && tocbo.length() > 0)
		{
			where = (where == null)? " where ": where + " and ";
			where += cbokey + " < '" + tocbo + "'";
		}

		return basesql + where;
	}
	
	public static String rs2cbokey(CBOMeta[] fields, ResultSet rs) throws Exception
	{
		String cbokey = null;
		for (CBOMeta m:fields)
		{
			if (!m.isCboKey) continue;
			String val = rs.getString(m.name);
			if (val != null) val = val.trim();
			cbokey = (cbokey == null)? val: cbokey + "/" + val;
		}
		return cbokey;
	}
	
}
