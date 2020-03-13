package com.bigid.connectors.insert;

import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcUtill {

	public static Connection getConnection() throws Exception {
		Class.forName(MySqlConstant.driverClassName);
		return DriverManager.getConnection(MySqlConstant.url, MySqlConstant.uname, MySqlConstant.password);
	}

}
