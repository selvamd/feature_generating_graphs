package fgg.grpc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.*;
import java.io.*;

import fgg.access.*;

/**
 *
 * @author sqlitetutorial.net
 */
public class FggSetupDb {

    public static Connection connectDB() throws Exception {
        return Persistor.getConnectionWithRetries(Persistor.qdburl());
    }

    public static void createTable(Connection conn, String sql) throws Exception
    {
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    public static void main(String[] args) throws Exception
    {
        Connection conn = connectDB();
        List<String> creates = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new FileReader("conf//cbo_derived.sql"));
        String aline = null;
        while ((aline = reader.readLine()) != null)
            creates.add(aline);
        aline = "";
        for (String s:creates) {
            aline += s;
            if (!s.endsWith(";")) continue;
            createTable(conn, aline);
            //System.out.println(aline);
            aline = "";
        }
    }
}
