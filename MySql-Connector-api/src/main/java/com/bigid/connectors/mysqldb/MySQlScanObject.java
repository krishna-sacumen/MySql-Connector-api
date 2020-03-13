package com.bigid.connectors.mysqldb;

import java.util.List;

import com.bigid.connectors.api.model.DataSourceObject;
import com.bigid.connectors.api.model.FieldInfo;

/**
 * 
 * @author Sacumen (www.sacumen.com)
 * Creating DataSourceObject using scan results.
 *
 */
public class MySQlScanObject implements DataSourceObject {

	String dsConnectionName;
	String tableName;
	String dbName;
	String fields;
	List<FieldInfo> fieldInfo;
	
	public MySQlScanObject(String dsConnectionName, String tableName, String dbName, List<FieldInfo> fieldInfo, String fields) {
		this.dsConnectionName = dsConnectionName;
		this.dbName = dbName;
		this.tableName = tableName;
		this.fields = fields;
		this.fieldInfo = fieldInfo;
	}

	@Override
	public String getDataSourceName() {
		return dsConnectionName;
	}

	@Override
	public String getContainerName() {
		return dbName;
	}

	@Override
	public String getObjectName() {
		return tableName;
	}

	@Override
	public String getFieldNamesAsDelimitedString() {
		return fields;
	}

	@Override
	public List<FieldInfo> getFieldsInfo() {
		return fieldInfo;
	}

}
