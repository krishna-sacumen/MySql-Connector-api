package com.bigid.connector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.bigid.connectors.insert.JdbcUtill;

public class MysqlConnectorWithJDBC {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		try {
		JSONObject object=null;
		Connection connection=JdbcUtill.getConnection();
		PreparedStatement statement = connection.prepareStatement("select * from user");
		ResultSet resultSet = statement.executeQuery();
		JSONArray jsonArray = new JSONArray();
		ResultSetMetaData metadata = resultSet.getMetaData();
		int columnCount = metadata.getColumnCount();
		while (resultSet.next()) {
			object = new JSONObject();
			for (int i = 1; i <= columnCount; i++) {
				object.put(metadata.getColumnName(i), resultSet.getString(metadata.getColumnName(i)));
			}
			object.put("Table Name", metadata.getTableName(1));
			object.put("DataBase Name", metadata.getCatalogName(1));
			jsonArray.add(object);
		}
	} catch (Exception e) {
		e.printStackTrace();
	}

	}

}
