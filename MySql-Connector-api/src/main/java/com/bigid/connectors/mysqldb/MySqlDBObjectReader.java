package com.bigid.connectors.mysqldb;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import com.bigid.connectors.api.DataSourceObjectReader;
import com.bigid.connectors.api.datalink.DataLink;
import com.bigid.connectors.api.model.DataSourceField;
import com.bigid.connectors.api.model.DataSourceRecord;
import com.bigid.connectors.api.model.Field;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @author Sacumen (www.sacumen.com) Create DataSourceObjectReader
 *
 */
@Slf4j
public class MySqlDBObjectReader implements DataSourceObjectReader {

	private ResultSet resultSet;
	private String tableName;
	private String dbName;
	private ResultSet currentResultSet;
	private MySqlUtil util = new MySqlUtil();

	public MySqlDBObjectReader(ResultSet resultSet, String dbName, String collectionName) {
		this.resultSet = resultSet;
		this.tableName = collectionName;
		this.dbName = dbName;
	}

	@Override
	public boolean nextRecord() throws IOException {
		log.info("getting next record..");
		if (resultSet.next()) {
			currentResultSet = resultSet;
			return true;
		}
		return false;
	}

	@Override
	public DataSourceRecord getCurrentRecord() throws IOException {
		log.info("getting current record...");
		ResultSet doc = currentResultSet;
		String recordId = getRecordId(doc);

		List<Field> fields = new ArrayList<>();
		addAllFieldsFromResult(doc, this.dbName, this.tableName, recordId, fields);
		return new DataSourceRecord(recordId, fields);
	}

	private String getRecordId(ResultSet result) {
		return result.getString(0);
	}

	/**
	 * Adding fields to the datasource record.
	 * 
	 * @param resultSet ResultSet to add in DataSourceRecord for scan
	 * @param dbName    Database name
	 * @param tableName Table name
	 * @param recordId  Record Id(first column of the table)
	 * @param fields    list of columns info
	 */
	private void addAllFieldsFromResult(ResultSet resultSet, String dbName, String tableName, String recordId,
			List<Field> fields) {
		log.info("adding all fields from result set ...");

		List<StructField> listOfFields = resultSet.getType().getStructFields();

		MySqlUtil util = new MySqlUtil();

		// starting from 1st index. zeroth index is record id.
		for (int i = 1; i < listOfFields.size(); i++) {
			StructField structField = listOfFields.get(i);
			String fieldName = structField.getName();
			String fieldType = structField.getType().toString();
			String fieldValue = util.getFieldValue(fieldName, resultSet);
			DataLink dataLink = new MySqlDBDataLink(dbName, tableName, recordId, fieldName, fieldType);
			Field field = new DataSourceField(fieldName, fieldValue, dataLink, fieldType);
			fields.add(field);
		}
	}

	@Override
	public void close() throws IOException {
		log.info("spanner DB result set close");
		if (resultSet != null) {
			resultSet.close();
		}
	}
}
