import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.*;

public class Perf {

    public static Connection connectDB(String fileName) throws Exception {
        String url = "jdbc:sqlite:" + fileName;
        return DriverManager.getConnection(url);
    }

    public static void createIndex(Connection conn, String cols) throws Exception
    {
        String sql = "CREATE INDEX "+cols+"_idx ON links("+cols+")";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    public static void createTable(Connection conn) throws Exception
    {
        //onoffdtcsv - For index 0, Activation is ON, for the rest it toggles
        String sql = "CREATE TABLE IF NOT EXISTS links (\n"
                + "	pk integer PRIMARY KEY,\n"
                + "	k0 integer NOT NULL,\n"
                + "	k1 integer NOT NULL,\n"
                + "	k2 integer NOT NULL,\n"
                + "	k3 integer NOT NULL,\n"
                + "	k4 integer NOT NULL,\n"
                + "	onoffdtcsv varchar(255)"
                + ");";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    public static void upsert(Connection conn, int from, int to) throws Exception
    {
        String sql = "INSERT or replace INTO links VALUES(?,?,?,?,?,?,?)";
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(sql);
		byte[] bytes = new byte[255];

        for (int i=from;i<=to;i++)
        {
            if (i % 1000 == 0) {
                pstmt.executeBatch();
                if (i%100000 == 0)
                    System.out.println("Written " + i + " records" );
            }
            for (int j=1;j<7;j++)
                pstmt.setInt(j,i);

			for (int k=0;k<255;k++)
				bytes[k] = (byte)(i%255);
			pstmt.setBytes(7,bytes);

            pstmt.addBatch();
        }
        pstmt.executeBatch();
        conn.commit();
        pstmt.close();
    }

    static AtomicInteger counter = new AtomicInteger(0);
    public static int select(Connection conn, int i) throws Exception
    {
        //String sql = "SELECT * FROM warehouses where id < " + i;

        String sql = "SELECT * FROM links where pk > " + (i * 100000) + " and pk <= " + ((i+1) * 100000);
        Statement stmt  = conn.createStatement();
        ResultSet rs    = stmt.executeQuery(sql);
        int count = 0;
        while (rs.next())
            count++;
        //System.out.println(count);
        //rs.getInt("id");
        rs.close();
        stmt.close();
        return count;
    }

	public static Callable<Integer> getCallable(final Connection conn) {
		return new Callable<Integer>() {
			public Integer call() throws Exception
			{
				try {
                    int count = 0;
                    for (int i=0;i<100;i++) {
                        select(conn, i);
                        count++;
                    }
                    return count;
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}
			}
		};

    }

    public static void main(String[] args) throws Exception
    {
        Connection conn = connectDB("perftest.db");
        createTable(conn);
        long l = System.currentTimeMillis();
        upsert(conn,1,20000000);
        System.out.println("upsert time " + (System.currentTimeMillis() - l)/1000);
        int thcnt = (args.length <= 0)? 5:Integer.parseInt(args[0]);
		    ExecutorService execsvc = Executors.newFixedThreadPool(thcnt);
        List<Future<Integer>> execs = new ArrayList<Future<Integer>>();

        long l0 = System.currentTimeMillis();
        for (int i=0;i<thcnt;i++)
            execs.add(execsvc.submit(getCallable(connectDB("perftest.db"))));
        int count = 0;
		for (Future<Integer> f:execs)
		{
			try {
				count += f.get();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
        long l1 = System.currentTimeMillis();
        System.out.println("Time gap = " + (l1-l0) + ",count=" + count + ",speed=" + ((l1-l0)/count));
        execsvc.shutdown();
    }
}
