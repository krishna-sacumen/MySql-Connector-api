package com.bigid.connector.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import com.bigid.connectors.api.BigIDException;
import com.bigid.connectors.api.DataSourceConnection;
import com.bigid.connectors.api.DataSourceObjectReader;
import com.bigid.connectors.api.QueryConditionNode;
import com.bigid.connectors.api.ScanContext;
import com.bigid.connectors.api.datalink.DataLink;
import com.bigid.connectors.api.model.BasicDataSourceObject;
import com.bigid.connectors.api.model.DataSourceObject;
import com.bigid.connectors.mysqldb.MySqlDBScanner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
@Ignore
public class MySqlDBScannerTest {
	
	private String dbName = "test";
	private String tableName = "test2";
	private String projectId = "big-id-conncetor-266810";
	private String instanceId = "big-id-connector";
	private String credentialsFile = "auth.json";
	
	@Mock
	private MySqlDBScanner spannerDBScanner;
	
	@Before
	public void initialize()  throws Exception {
		MockitoAnnotations.initMocks(this);
		spannerDBScanner = new MySqlDBScanner();
		spannerDBScanner.init(createDummyScanContext());
	}
	
	private ScanContext createDummyScanContext() throws IOException {
		DataSourceConnection connection = new DataSourceConnection();
		connection.set("spanner_db_name", dbName);
		connection.set("spanner_project_id", projectId);
		connection.set("spanner_instance_id", instanceId);
		connection.set("spanner_auth_json", IOUtils.toString(getClass().getClassLoader().getResourceAsStream(credentialsFile), StandardCharsets.UTF_8));
		ScanContext dummyContext;
		dummyContext = new ScanContext();
		dummyContext.setConnection(connection);
		dummyContext.setScanId("Test");
		return dummyContext;
	}
}