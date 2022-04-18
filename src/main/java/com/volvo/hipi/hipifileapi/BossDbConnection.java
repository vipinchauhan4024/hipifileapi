package com.volvo.hipi.hipifileapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BossDbConnection {

    private BossDbConnection(){}
    Connection conn;
    public static Connection getDbConnection (){
        Connection conn= null;
        if(conn != null){
            return conn;
        }
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            //Creating a connection to the
            conn = DriverManager.getConnection("jdbc:jtds:sqlserver://SEGOTN13190/DPROTUST:1433", "protusadm", "protustest");

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
 return conn;

    }
}
