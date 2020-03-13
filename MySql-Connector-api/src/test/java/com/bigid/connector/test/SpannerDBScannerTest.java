package com.bigid.connector.test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import com.bigid.connectors.api.BigIDException;
import com.bigid.connectors.api.DataSourceConnection;
import com.bigid.connectors.api.DataSourceObjectReader;
import com.bigid.connectors.api.QueryConditionNode;
import com.bigid.connectors.api.ScanContext;
import com.bigid.connectors.api.model.BasicDataSourceObject;
import com.bigid.connectors.api.model.DataSourceObject;
import com.bigid.connectors.api.model.FieldInfo;
import com.bigid.connectors.api.utils.DataSourceSupportedFeatures;
import com.bigid.connectors.mysqldb.MySQlScanObject;
import com.bigid.connectors.mysqldb.MySqlDBConnection;
import com.bigid.connectors.mysqldb.MySqlDBConnectionManager;
import com.bigid.connectors.mysqldb.MySqlDBDataLink;
import com.bigid.connectors.mysqldb.MySqlDBScanner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;

@RunWith(MockitoJUnitRunner.class)
public class SpannerDBScannerTest {
	
	@InjectMocks
	@Spy
	private MySqlDBScanner dBScannerSpy;

	@Mock
	private MySqlDBConnectionManager dBConnectionManager;

	@Mock
	private MySQlScanObject dataSourceObject;

	@Mock
	private MySqlDBDataLink dBDataLink;

	@Mock
	private MySqlDBConnection dBConnection;

	ScanContext scanContext;

	String dbName = "user";

	@Test
	private ScanContext createDummyScanContext() throws IOException {
		DataSourceConnection connection = new DataSourceConnection();
		connection.set("spanner_db_name", dbName);
		ScanContext dummyContext;
		dummyContext = new ScanContext();
		dummyContext.setConnection(connection);
		return dummyContext;
	}
	
}
