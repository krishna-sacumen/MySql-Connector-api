package com.bigid.connectors.mysqldb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.bigid.connectors.api.model.FieldInfo;
import com.bigid.connectors.insert.JdbcUtill;
import com.google.cloud.spanner.DatabaseClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlDBConnectionManager {

	private MySqlDBConnection dbConnection;
	private List<FieldInfo> listOfFieldInfo;

	public MySqlDBConnectionManager(MySqlDBConnection dbConnection) throws IOException {
		this.dbConnection = dbConnection;
	}

	public ResultSet getResultSet(String finalQuery) {
		log.info("getting result set...");
		log.info("final query is " + finalQuery);
		try {
			ResultSet resultSet = JdbcUtill.getConnection().prepareStatement(finalQuery).executeQuery();
			return resultSet;
		} catch (Exception e) {
			log.error("exception while executing statement: ", e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public List<String> getAllDbNames() {
		log.info("getting all db names..");
		List<String> stringList = new ArrayList<>();
		try {
			Statement statement = JdbcUtill.getConnection().createStatement();
			ResultSet resultSet = statement.executeQuery("Show Databases");
			while (resultSet.next()) {
				stringList.add(resultSet.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return stringList;
	}

	public List<FieldInfo> getListOfFields(Connection connection, String sqlQuery) {
		log.info("getListColumnsName");
		// listOfFieldInfo
		try {
			ResultSetMetaData metadata = connection.prepareStatement(sqlQuery).executeQuery().getMetaData();
			int columnCount = metadata.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				listOfFieldInfo.add(new FieldInfo(metadata.getColumnName(i), metadata.getColumnTypeName(i), false));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listOfFieldInfo;
	}

	/**
	 * 
	 * @return list with all column names
	 */
	public List<String> getColumnNames() {
		return listOfFieldInfo.stream().map(field -> field.getName()).collect(Collectors.toList());
	}

	/**
	 * 
	 * @param resultSet
	 * @return Number of records fetch from sql statement.
	 * @throws SQLException
	 */
	public int getCount(ResultSet resultSet) throws SQLException {
		int count = 0;
		while (resultSet.next()) {
			count++;
		}
		return count;
	}
	
	/**
	 * 
	 * @param dbname Database name
	 * @param query  Sql query statement which will add in where clause
	 * @return result sets with matching condition.
	 */
	public ResultSet getResultSetForCondition(String query) {
		log.info("getting result set..");
		try {
			ResultSet resultSet = JdbcUtill.getConnection().createStatement().executeQuery(query);
			return resultSet;
		} catch (Exception e) {
			log.error("Error while fetching result set", e);
		}
		return null;
	}
	
	/**
	 * 
	 * @param dbname    Database name
	 * @param tableName table name
	 * @param id        get result set for specific id
	 * @return return result sets for specific id.
	 */
	public ResultSet getResultSetForId(String tableName, String id) {
		
		try {
			PreparedStatement preparedStatement= JdbcUtill.getConnection().prepareStatement("select * from "+tableName + " where user_id" +id);
			preparedStatement.setString(1, id);
			ResultSet resultSet=preparedStatement.executeQuery();
			return resultSet;
		} catch (Exception e) {
			log.error("Error while fetching result set", e);
		}
		return null;
	}

}
