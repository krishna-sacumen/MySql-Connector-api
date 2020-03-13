package com.bigid.connectors.insert;

import java.sql.Statement;

public class InsertIntable {

	public static void main(String[] args) throws Exception {
		JdbcUtill util = new JdbcUtill();
		try {

			Statement statement = util.getConnection().createStatement();
			String sql = "insert into user values" + "(1, 'manoj', 'prabhu','manoj@gmail.com')";
			int value = statement.executeUpdate(sql);
			System.out.println("records affected : " + value);
		} catch (Exception e) {
			e.printStackTrace();
			util.getConnection().close();
		}

		System.out.println("------------------");
		try {
			Statement statement = util.getConnection().createStatement();
			String sql = "insert into request values" + "(1, '5', 'U.P','9000','India')";
			int value = statement.executeUpdate(sql);
			System.out.println("records affected : " + value);
		} catch (Exception e) {
			e.printStackTrace();
			util.getConnection().close();
		}
	}

}
