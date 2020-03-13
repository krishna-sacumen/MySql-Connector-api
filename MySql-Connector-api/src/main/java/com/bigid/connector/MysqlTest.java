package com.bigid.connector;

import java.sql.Connection;
import java.sql.ResultSet;

import com.bigid.connectors.insert.JdbcUtill;

public class MysqlTest {
	@SuppressWarnings("unlikely-arg-type")
	public static void main(String[] args) throws Exception {
		ResultSet resultSet = JdbcUtill.getConnection().createStatement().executeQuery("select * from user");
		MysqlTest mysqlTest = new MysqlTest();
		String value = mysqlTest.getFieldValues(resultSet);
		JdbcUtill.getConnection().close();
	}

	public String getFieldValues(ResultSet resultSet) {
		Connection connection = null;
		java.sql.ResultSetMetaData rs = null;
		try {
			connection = JdbcUtill.getConnection();
			rs = resultSet.getMetaData();
			int columnCount = rs.getColumnCount();
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					// equals(rs.getColumnTypeName(i))
					String value = "";
					if (rs.getColumnTypeName(i).equalsIgnoreCase("INT UNSIGNED")) {
						value = rs.getColumnTypeName(i).substring(0, rs.getColumnTypeName(i).length() - 9);
					}
					if (MySqlTypeEnum.INTEGER.toString().equalsIgnoreCase(value)) {
						return String.valueOf(resultSet.getLong(rs.getColumnName(i)));
					} else if (MySqlTypeEnum.VARCHAR.toString().equalsIgnoreCase(rs.getColumnTypeName(i))) {
						return String.valueOf(resultSet.getString(rs.getColumnName(i)));
					} else if (MySqlTypeEnum.DATE.toString().equalsIgnoreCase(rs.getColumnTypeName(i))) {
						return String.valueOf(resultSet.getDate(rs.getColumnName(i)));
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
