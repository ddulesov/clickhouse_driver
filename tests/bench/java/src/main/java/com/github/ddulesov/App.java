package com.github.ddulesov;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Map;

public class App 
{
    static void insert(Connection connection) throws URISyntaxException, SQLException{
        final Statement stmt = connection.createStatement();
        stmt.executeQuery("DROP TABLE IF EXISTS perf_java");
        stmt.executeQuery("CREATE TABLE IF NOT EXISTS perf_java (" +
                "    id  UInt32," +
                "    name  String," +
                "    dt   DateTime " +
                ") Engine=MergeTree PARTITION BY name ORDER BY dt");
        stmt.close();

        final String[] Names =  {"one","two","three","four","five"};

        long startTime = System.nanoTime();
        final PreparedStatement pstmt = connection.prepareStatement("INSERT INTO perf_java VALUES(?, ?, ?)");

        if(pstmt==null){
            return;
        }

        long now = System.currentTimeMillis();
        for (int j = 1; j <= 1000; j++) {
            for (int i = 1; i <= 10000; i++) {
                pstmt.setInt(1,i);
                pstmt.setString( 2, Names[ i % Names.length]  );
                pstmt.setTimestamp(3,    new java.sql.Timestamp(now + i ) );
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.clearBatch();
        }

        pstmt.close();

        long stopTime = System.nanoTime();
        System.out.format("elapsed %d msec", (stopTime - startTime)/1000000);
        connection.close();
    }

    static void select(Connection connection) throws URISyntaxException, SQLException{
        long startTime = System.nanoTime();
        final Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id, name, dt FROM perf");
        long sum = 0;

        while (rs.next()) {
            sum += rs.getInt(1);
            final String name = rs.getString(2);
            final java.sql.Timestamp time = rs.getTimestamp(3);
        }

        long stopTime = System.nanoTime();
        System.out.format("elapsed %d msec\n", (stopTime - startTime)/1000000);
        System.out.format("sum %d\n",sum);
        connection.close();
    }

    public static void main( String[] args ) throws URISyntaxException, SQLException {
        Map<String, String> env = System.getenv();
        final String url = env.containsKey("DATABASE_URL")?
            env.get("DATABASE_URL"): "clickhouse://127.0.0.1:9000";

        // transform a url to an adopted  for this driver form
        final URI uri = URI.create(url);

        String username = uri.getUserInfo();
        String password = "";
        String query = uri.getQuery();

        String[] userinfo = uri.getUserInfo().split(":");
        if ( userinfo.length>1 ){
            username = userinfo[0];
            password = userinfo[1];
            query = query + "&password="+password;
        }

        final URI uri2 = new URI("jdbc:clickhouse", username,  uri.getHost(),
                (uri.getPort()==-1)?9000: uri.getPort(),
                uri.getPath(),
                query,
                uri.getFragment());
        // Show resulted url
        System.out.format("url: '%s'\n",uri2);

        final Connection connection = DriverManager.getConnection(uri2.toString());

        if (args.length>1 && args[1].equals("insert")){
            App.insert(connection);
            return;
        }
        if (args.length>1 && args[1].equals("select")){
            App.select(connection);
            return;
        }
        System.err.println("specify bench method `select` or `insert` in the first argument");

    }
}
