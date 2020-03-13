package com.bigid.connectors.insert;

public class MySqlConstant {

	public static String url="jdbc:mysql://localhost:3306/demo";
	public static String uname="root";
	public static String password="password"; 
	public static String driverClassName="com.mysql.jdbc.Driver";
	
	public static String sql_With_Id="select * from request where req_id=?";

	public static String sql = "select * from user";
}
