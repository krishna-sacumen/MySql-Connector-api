package com.bigid.connectors.mysqldb;

import com.bigid.connectors.api.datalink.ConnectorDataLink;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;


@EqualsAndHashCode
@Accessors(chain = true)
@Data
public class MySqlDBDataLink extends ConnectorDataLink{
	private String dbName;
    private String tableName;
    private String id;
    private String fieldName;
    private String shortFieldName;

    public MySqlDBDataLink(String dbName, String tableName, String id, String fieldName, String mdFieldName) {
        this(dbName, tableName, id, fieldName, mdFieldName, 0);
    }

    public MySqlDBDataLink(String dbName, String tableName, String id, String fieldName, String mdFieldName, long position) {
        this.dataLinkType = "google-spanner-db";
        this.dbName = dbName;
        this.tableName = tableName;
        this.id = id;
        this.fieldName = fieldName;
        this.shortFieldName = createShortFieldName(fieldName);
    }

    public MySqlDBDataLink(long position) {
        this.dataLinkType = "google-spanner-db";
        this.position = position;
    }

    public MySqlDBDataLink copy() {
        return new MySqlDBDataLink(position)
                .setDbName(dbName)
                .setTableName(tableName)
                .setId(id)
                .setFieldName(fieldName)
                .setShortFieldName(shortFieldName);
    }

    private String createShortFieldName(String fieldName) {
        if (fieldName == null) {
            return "";
        }
        String[] fieldNameParts = fieldName.split("\\.");
        return fieldNameParts.length > 1 ? fieldNameParts[0] + "." + fieldNameParts[1] : fieldNameParts[0];
    }

}
