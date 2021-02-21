package hive.com;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HIveJdbcClient {
    private final static String dirverName= "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        Class.forName(dirverName);

        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.0.21:10000/default", "kevin", "");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from partable1");
        while(resultSet.next()){
            System.out.println(resultSet.getString(1)+"-"+resultSet.getInt(2));
        }


    }
}
