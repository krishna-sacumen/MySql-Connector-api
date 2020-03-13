package com.bigid.connectors.mysqldb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.bigid.connectors.api.GeneralConnection;
import com.bigid.connectors.api.ScanContext;
import com.bigid.connectors.insert.MySqlConstant;

/**
 * 
 * @author Sacumen (www.sacumen.com) Class to set/get connection parameters
 *
 */
public class MySqlDBConnection extends GeneralConnection {

	private String dbName;
	private String dsConnectionName;
	private String tableName;
	private ResultSet resultSet;

	@SuppressWarnings("unchecked")
	public MySqlDBConnection(ScanContext scanContext) throws SQLException {
		Connection connection = scanContext.getMySqlConnection();
		String sql_query = null;
		JSONObject object = null;
		int id = 0;

		if (id > 0) {
			sql_query = MySqlConstant.sql_With_Id;
		} else {
			sql_query = MySqlConstant.sql;
		}
		PreparedStatement statement = connection.prepareStatement(sql_query);
		if (id > 0) {
			statement.setInt(1, id);
		}
		resultSet = statement.executeQuery();
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
		tableName = (String) object.get("Table Name");
		dbName = (String) object.get("DataBase Name");
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getSource() {
		return dsConnectionName;
	}

	public void setSource(String dsConnectionName) {
		this.dsConnectionName = dsConnectionName;
	}

	public String getTableName() {
		return tableName;
	}

	public boolean nextRecord() throws IOException, SQLException {
		if (resultSet.next()) {
			return true;
		}
		return false;
	}

}
