package com.bigid.connectors.mysqldb;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Objects;

import java.util.stream.Collectors;

import com.bigid.connector.MySqlTypeEnum;
import com.bigid.connectors.insert.JdbcUtill;

/**
 * 
 * @author Sacumen (www.sacumen.com) Utility class to expose some common method
 *         used in the project.
 *
 */
public class MySqlUtil {

	/**
	 * 
	 * @param fieldName
	 *            Column name
	 * @param resultSet
	 *            ResultSet
	 * @return Column value
	 */
	public String getFieldValue(ResultSet resultSet) {
		ResultSetMetaData rsmd = null;
		try {
			rsmd = resultSet.getMetaData();
			int columnCount = rsmd.getColumnCount();
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					// equals(rs.getColumnTypeName(i))
					String value = "";
					if (rsmd.getColumnTypeName(i).equalsIgnoreCase("INT UNSIGNED")) {
						value = rsmd.getColumnTypeName(i).substring(0, rsmd.getColumnTypeName(i).length() - 9);
					}
					if (MySqlTypeEnum.INTEGER.toString().equalsIgnoreCase(value)) {
						return String.valueOf(resultSet.getLong(rsmd.getColumnName(i)));
					} else if (MySqlTypeEnum.VARCHAR.toString().equalsIgnoreCase(rsmd.getColumnTypeName(i))) {
						return String.valueOf(resultSet.getString(rsmd.getColumnName(i)));
					} else if (MySqlTypeEnum.DATE.toString().equalsIgnoreCase(rsmd.getColumnTypeName(i))) {
						return String.valueOf(resultSet.getDate(rsmd.getColumnName(i)));
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
