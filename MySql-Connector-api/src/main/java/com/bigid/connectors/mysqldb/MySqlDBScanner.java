package com.bigid.connectors.mysqldb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.bigid.connectors.api.BigIDException;
import com.bigid.connectors.api.DataSourceConnector;
import com.bigid.connectors.api.DataSourceObjectReader;
import com.bigid.connectors.api.IdScanContext;
import com.bigid.connectors.api.IdentitySourceConnector;
import com.bigid.connectors.api.JITField;
import com.bigid.connectors.api.QueryConditionNode;
import com.bigid.connectors.api.ScanContext;
import com.bigid.connectors.api.datalink.DataLink;
import com.bigid.connectors.api.model.BasicDataSourceObject;
import com.bigid.connectors.api.model.DataSourceObject;
import com.bigid.connectors.api.utils.DataSourceSupportedFeatures;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlDBScanner implements DataSourceConnector, IdentitySourceConnector {

	private MySqlDBConnection dBConnection;
	private Connection connection;
	private List<String> dbNames;
	private ScanContext scanContext;
	private MySqlDBConnectionManager dBConnectionManager;

	public MySqlDBScanner() {
	}

	@Override
	public void init(ScanContext scanContext) {
		log.info("initialization...");
		try {
			dBConnection = new MySqlDBConnection(scanContext);
			dBConnectionManager = getDBConnectionManager();

		} catch (Exception e) {
			e.printStackTrace();
		}
		setDBNames();

	}

	public MySqlDBConnectionManager getDBConnectionManager() throws IOException {
		log.info("getting db connection managet.........");

		return new MySqlDBConnectionManager(dBConnection);
	}

	@SuppressWarnings("unchecked")
	private void setDBNames() {
		log.info("setting db name..");
		log.debug("dbname is " + dBConnection.getDbName());
		Optional<String> dbName = Optional.ofNullable(dBConnection.getDbName()).filter(name -> !name.isEmpty());
		dbNames = dbName.map(Collections::singletonList).orElseGet(() -> dBConnectionManager.getAllDbNames());
	}

	@Override
	public Iterator<DataSourceObject> getDataSourceObjects(ScanContext scanContext) throws BigIDException {
		log.info("getting data source object......");
		Set<String> userTables = getUserSelectedTable();
		List<DataSourceObject> dataSourceObjectList = dbNames.stream()
				.flatMap(name -> createSpannerScanObject(name, userTables).stream()).collect(Collectors.toList());
		return dataSourceObjectList.iterator();
	}

	@Override
	public EnumSet<DataSourceSupportedFeatures> getSupportedFeatures() {
		log.info("getting supported features..");
		return EnumSet.of(DataSourceSupportedFeatures.HasStructure);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public DataSourceObject getDataSourceObject(IdScanContext idScanContext, String objectName) {
		log.info("getting data source object");
		String dbName = "";
		String tableName = "";
		if (dbNames.size() == 1) { // DS connection contain 1 DB name
			dbName = dbNames.get(0);
			tableName = objectName;
		} else if (objectName.contains(".")) {
			String[] arr = objectName.split("\\.");
			dbName = arr[0];
			tableName = arr[1];
		} else
			throw new IllegalArgumentException("ERROR on setting DB name. Please add at DB Table: <DBName>.<DBTable>");

		return new MySQlScanObject(dBConnection.getSource(), tableName, dbName,
				dBConnectionManager.getListOfFields(connection, tableName),
				String.join(",", dBConnectionManager.getColumnNames()));

	}

	@Override
	public DataSourceObjectReader createIDSourceObjectReaderByQuery(String normalizedQuery, IdScanContext context)
			throws BigIDException {
		log.info("createIDSourceObjectReaderByQuery");
		return null;
	}

	@Override
	public MySqlDBObjectReader createIDSourceObjectReader(DataSourceObject object, long pageSize,
			QueryConditionNode conditionTree, IdScanContext context) throws BigIDException {

		log.info("creating id source object reader..");
		MySQlScanObject scanObject = (MySQlScanObject) object;
		ResultSet collectionToScan = null;
		if (conditionTree != null || pageSize > 0) {
			String query = getIdSourceScanDefaultQuery(object, pageSize, conditionTree);
			collectionToScan = dBConnectionManager.getResultSetForCondition(query);
		} else {
			collectionToScan = dBConnectionManager.getResultSet(scanObject.getObjectName());
		}
		return new MySqlDBObjectReader(collectionToScan, scanObject.getContainerName(), scanObject.getObjectName());

	}

	@Override
	public String normalizeQuery(String customQuery, long pageSize, QueryConditionNode conditionTree) {
		log.info("normalizeQuery");
		return null;
	}

	@Override
	public long count(IdScanContext idScanContext, DataSourceObject object) throws BigIDException {
		log.info("count...with query");
		return 0;
	}

	@Override
	public long count(IdScanContext idScanContext, String query) throws BigIDException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DataSourceObjectReader createDataSourceObjectReader(DataSourceObject dataSourceObject,
			ScanContext scanContext) throws BigIDException {
		log.info("creating data source object reader......");
		MySQlScanObject scanObject = (MySQlScanObject) dataSourceObject;
		ResultSet collectionToScan = dBConnectionManager.getResultSet(scanObject.getObjectName());
		return new MySqlDBObjectReader(collectionToScan, scanObject.getContainerName(), scanObject.getObjectName());
	}

	@Override
	public DataSourceObjectReader createDataSourceObjectJITReader(BasicDataSourceObject dataSourceObject,
			List<JITField> jitFields, ScanContext scanContext) throws BigIDException {
		log.info("creating data source object JIT reader...");
		ResultSet collectionToScan = dBConnectionManager.getResultSet(dataSourceObject.getObjectName());
		return new MySqlDBObjectReader(collectionToScan, dataSourceObject.getContainerName(),dataSourceObject.getObjectName());
	}

	@Override
	public String fetchClearValue(ScanContext scanContext, DataLink dataLink) throws BigIDException {
//		ResultSet resultSet;
//
//		MySqlDBDataLink dBDataLink = (MySqlDBDataLink) dataLink;
//		
//		ResultSet collectionToFetchFrom = dBConnectionManager.getResultSetForId(dBDataLink.getTableName(), dBDataLink.getId());
//
//		if (collectionToFetchFrom != null && collectionToFetchFrom.next()) {
//			resultSet = collectionToFetchFrom;
//		} else {
//			log.error("Error while fetching result using id");
//			return null;
//		}
//		MySqlUtil util = new MySqlUtil();
//		String recursiveFieldName = dBDataLink.getFieldName();
//		String[] splittedFieldNames = recursiveFieldName.split("\\.");
//		Stream<String> stream = Stream.of(splittedFieldNames);
//		List listOfFieldValue = stream.filter(field -> field != null).map(field -> util.getFieldValue(field, resultSet))
//				.collect(Collectors.toList());
//
//		return String.join(",", listOfFieldValue);
		return null;
	}

	@Override
	public Class getDataLinkClass() {
		log.info("getDataLinkClass..");
		return MySqlDBDataLink.class;
	}

	@Override
	public boolean hasNativeSampling() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * 
	 * @return List of table names defined by user.
	 */
	public Set<String> getUserSelectedTable() {
		log.info("gettting user selectable tables....");
		Set<String> userTables = new HashSet<>();
		String commaSepTablesList = dBConnection.getTableName();
		if (commaSepTablesList != null && commaSepTablesList.length() > 0) {
			StringTokenizer st = new StringTokenizer(commaSepTablesList, ",");
			while (st.hasMoreTokens())
				userTables.add(st.nextToken());
		}
		return userTables;
	}

	/**
	 * 
	 * @param dbName
	 *            Database name
	 * @param userTables
	 *            Table list(comma separated string) given by user.
	 * @return list of DataSourceObject
	 */
	private List<DataSourceObject> createSpannerScanObject(String dbName, Set<String> userTables) {
		log.info("creating spanner scan object..");
		Iterable<String> iterable = getListOfTableName(connection);
		return StreamSupport.stream(iterable.spliterator(), false)
				.filter(collectionName -> tableNameInUserTables(userTables, collectionName))
				.map(collectionName -> new MySQlScanObject(dBConnection.getSource(), collectionName, dbName,
						dBConnectionManager.getListOfFields(connection, collectionName),
						String.join(",", dBConnectionManager.getColumnNames())))
				.collect(Collectors.toList());
	}

	public Iterable<String> getListOfTableName(Connection connection) {
		log.info("getListOfTableName");
		List<String> listofTable = new ArrayList<String>();
		try {
			DatabaseMetaData md = connection.getMetaData();
			ResultSet rs = md.getTables(null, null, "%", null);
			while (rs.next()) {
				if (rs.getString(4).equalsIgnoreCase("TABLE")) {
					listofTable.add(rs.getString(3));
				}
			}
		} catch (Exception e) {
			log.error("Error accur while fetching tables list from database: ", e);
		}
		return listofTable;
	}

	/**
	 * 
	 * @param userTables
	 *            Table list(comma separated string) given by user.
	 * @param tableName
	 *            Table name
	 * @return check if given table is present in user defined tables.
	 */
	private boolean tableNameInUserTables(Set<String> userTables, String tableName) {
		log.info("tableNameInUserTables");
		log.debug("tableName is " + tableName);
		return !(!userTables.isEmpty() && !userTables.contains(tableName));
	}

	/**
	 * 
	 * @param object
	 *            DataSourceObject
	 * @param limit
	 *            Limiting the records.
	 * @param conditionTree
	 *            condition for records
	 * @return query, which will add in sql where clause.
	 */
	protected String getIdSourceScanDefaultQuery(DataSourceObject object, long limit,
			QueryConditionNode conditionTree) {
		StringBuilder query = new StringBuilder("SELECT * FROM ").append(object.getObjectName());
		if (conditionTree != null) {
			query.append(" WHERE ").append(conditionTree.toString());
		}
		if (limit > 0) {
			query.append(" LIMIT ").append(limit);
		}
		return query.toString();
	}

}