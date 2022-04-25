package com.volvo.hipi.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BossDbConnection {

	private BossDbConnection() {
	}

	private static Connection conn = null;

	public static Connection getDbConnection() {
		if (conn != null) {
			return conn;
		}
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			// Creating a connection to the test
			 //conn =  DriverManager.getConnection("jdbc:jtds:sqlserver://SEGOTN13190/DPROTUST:1433", "protusadm","protustest");

			// Prod
			conn =  DriverManager.getConnection("jdbc:jtds:sqlserver://SEGOTN11810-001/DPROTUSP:1433", "admprotus","protusprod");
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return conn;

	}
}
