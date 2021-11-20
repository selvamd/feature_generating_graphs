package fgg.access;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.io.*;
import java.sql.*;
import java.text.*;
import fgg.data.*;

public class CBOPersistor extends Persistor
{

	public static String GenerateAccountKey(CBO c)
	{
		return GenerateAccountKey(c.get("business_line"), c.get("src_type_cd"), c.get("src_acct"));
	}

	public static String GenerateAccountKey(String bl, String src_cd, String src_acct)
	{
		if (bl == null || src_cd == null || src_acct == null)
			return null;

		bl 			= bl.trim().toUpperCase();
		src_cd 	 	= src_cd.trim().toUpperCase();
		src_acct 	= src_acct.trim().toUpperCase();

		if ("LOAN".equals(bl) || "DEPOSIT".equals(bl))
			return src_acct + "/" + src_cd;
		else
			return src_acct + "/" + bl;
	}

	public static int maxcboid(CBOMeta[] meta) throws Exception
	{
		return maxkey(meta[0].table);
	}

	public static Map<String,CBO> loadAllCBO(CBOMeta[] meta, String filter, String keys) throws Exception
	{
		Map<String,CBO> currcbos = new TreeMap<String,CBO>();
		CBOMeta[] mkeys = CBOMeta.makekey(meta, keys);
		if (mkeys == null) return currcbos;

		String sql = "select * from " + meta[0].table;
		if (filter != null && filter.length() > 0)
			sql += " where " + filter;

		executeQuery(qdburl(), sql, new QryProcessor() {
			public boolean process(ResultSet rs) throws Exception
			{
				CBO cbo = new CBO(meta);
				cbo.initQDB(rs);
				currcbos.put(cbo.cbokey(mkeys),cbo);
				return true;
			}
		});
		System.out.println("Loaded " + currcbos.size() + " " + meta[0].table + " records");
		return currcbos;
	}

	public static void storecsvheader(String filename, CBOMeta[] fields) throws Exception
	{
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
		String result = null;
		for (CBOMeta m:fields)
			result = (result == null)? m.name:result+","+m.name;
		writer.write(result+"\n");
		writer.close();
	}

	public static void storecsv(String filename, Collection<CBO> cbos) throws Exception
	{
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));
		int count = 0;
		for (CBO c:cbos)
		{
			writer.write(c.toCsv()+"\n");
			count++;
			if (count % 100000 == 0)
				System.out.println("Written " + count + " records");
		}
		System.out.println("Completed Writing all " + count + " records");
		writer.close();
 	}

	public static void storefile(String filename, Collection<CBO> cbos) throws Exception
	{
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));
		int count = 0;
		for (CBO c:cbos)
		{
			writer.write(c+"\n");
			count++;
			if (count % 100000 == 0)
				System.out.println("Written " + count + " records");
		}
		System.out.println("Completed Writing all " + count + " records");
		writer.close();
	}

	private static void executeBatchChecked(PreparedStatement stmt, Set<Integer> keys) {
		try {
			stmt.executeBatch();
		} catch (Exception e) {
			System.out.println("Batch update failed for keys " + keys);
			e.printStackTrace();
		} finally {
			keys.clear();
		}
	}

    public interface CBOFilter { public boolean process(CBO cbo);  };

	public static int persistnew(CBOMeta[] meta, Collection<CBO> cbos)  throws Exception { return persistnew(meta,cbos,null); }
	public static int persistnew(CBOMeta[] meta, Collection<CBO> cbos, CBOFilter filter) throws Exception { return persistnew(qdburl(), meta, cbos, filter, true); }
	public static int persistnew(String dburl, CBOMeta[] meta, Collection<CBO> cbos, CBOFilter filter, boolean verify) throws Exception
	{
		Set<Integer> keys = new HashSet<Integer>();
		if (cbos.size() == 0) return 0;
		int count = 0;
        Connection con = getConnectionWithRetries(dburl);
		PreparedStatement stmt = null;

		// Commit all the inserts herej
        stmt = con.prepareStatement(CBO.insertStmtSql(meta));
		con.setAutoCommit(false);

		for (CBO c: cbos)
		{
			if (verify && (!c.verify())) continue;
			if (filter != null && !filter.process(c))
					continue;
			c.persistnew(stmt);
			stmt.addBatch();
			keys.add(c.recid());
			if (++count % 1500 == 0)
			{
				executeBatchChecked(stmt,keys);
				con.commit();
			}
		}

		executeBatchChecked(stmt,keys);
		con.commit();

        stmt.close();
		con.close();

		return count;
	}

	//Will try to update or insert if it cannot.
	//strkey provices the list of fields used to check if update can happen
	public static int persistUpdateInsert(CBOMeta[] meta, Collection<CBO> cbos, String strkey)
		throws Exception
	{
		Set<Integer> keys = new HashSet<Integer>();
		if (cbos.size() == 0) return 0;
		int count = 0;
        Connection con = getConnectionWithRetries(qdburl());
		PreparedStatement stmt = null;

		// Commit all the inserts here
        stmt = con.prepareStatement(CBO.updateOrInsertStmtSql(meta, strkey));
		con.setAutoCommit(false);

		for (CBO c: cbos)
		{
			if (!c.verify()) continue;
			c.persistUpdateInsert(stmt, strkey);
			stmt.addBatch();
			if (++count % 2500 == 0)
			{
				executeBatchChecked(stmt,keys);
				con.commit();
			}
		}

		executeBatchChecked(stmt,keys);
		con.commit();

        stmt.close();
		con.close();

		return count;
	}

	//Runs a full update of all the fields
	public static int persistupdates(CBOMeta[] meta, Collection<CBO> cbos, CBOFilter filter)
		throws Exception
	{
		Set<Integer> keys = new HashSet<Integer>();
		if (cbos.size() == 0) return 0;
		int count = 0;
        Connection con = getConnectionWithRetries(qdburl());
		PreparedStatement stmt = null;

		// Commit all the inserts here
        stmt = con.prepareStatement(CBO.updateStmtSql(meta));
		con.setAutoCommit(false);

		for (CBO c: cbos)
		{
			if (!c.verify()) continue;
			if (filter != null && !filter.process(c))
					continue;
			c.persistupdate(stmt);
			stmt.addBatch();
			keys.add(c.recid());
			if (++count % 2500 == 0)
			{
				executeBatchChecked(stmt,keys);
				con.commit();
			}
		}

		executeBatchChecked(stmt,keys);
		con.commit();

        stmt.close();
		con.close();

		return count;
	}

	//Persist the updates to the to_date (or end date) field for the
	//existing current records that are not current any more
	public static int persistupdates4scd(CBOMeta[] meta, Collection<CBO> cbos)
		throws Exception
	{
		Set<Integer> keys = new HashSet<Integer>();
		if (cbos.size() == 0) return 0;
		int count = 0;

        Connection con = getConnectionWithRetries(qdburl());
		PreparedStatement stmt = null;

		// Commit all the inserts here
        stmt = con.prepareStatement(CBO.updateStmtSql4Scd(meta));
		con.setAutoCommit(false);

		for (CBO c: cbos)
		{
			if (!c.checked()) continue;
			if ("99999999".equals(c.get("to_dt"))) continue;
			c.persistupdate4scd(stmt);
			stmt.addBatch();
			keys.add(c.recid());

			if (++count % 1500 == 0)
			{
				executeBatchChecked(stmt,keys);
				con.commit();
			}
		}

		executeBatchChecked(stmt,keys);
		con.commit();

        stmt.close();
		con.close();

		return count;
	}

	public static CBOMeta[] buildMeta(String tablename, String[] keys, String[] groups) throws Exception
	{
		Connection con = getConnectionWithRetries(qdburl());
		Statement  stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("select * from " + tablename + " where 1 = 2");
		ResultSetMetaData rsmd = rs.getMetaData();

		List<CBOMeta> result = new ArrayList<CBOMeta>();
		if (keys == null) keys = new String[] {""};
		if (groups == null) groups = new String[] {""};
		for (int i=0;i<rsmd.getColumnCount();i++)
		{
			if (rsmd.getColumnTypeName(i+1).indexOf("binary") >= 0)
				continue;
			CBOMeta m = new CBOMeta();
			m.init(rsmd,i+1, keys, groups, tablename);
			result.add(m);
		}
		return (CBOMeta[])result.toArray(new CBOMeta[result.size()]);
	}

}
