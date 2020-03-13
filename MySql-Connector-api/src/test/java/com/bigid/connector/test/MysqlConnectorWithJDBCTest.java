package com.bigid.connector.test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.skyscreamer.jsonassert.JSONAssert;

import com.bigid.connector.MysqlConnectorWithJDBC;
import com.bigid.connectors.insert.JdbcUtill;

@RunWith(MockitoJUnitRunner.class)
public class MysqlConnectorWithJDBCTest {

	@Mock
	private ResultSet resultSet;

	@InjectMocks
	private MysqlConnectorWithJDBC connectorWithJDBC;
	JdbcUtill util=new JdbcUtill();
	
	@Before
	public void init() throws SQLException, ClassNotFoundException {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void check() throws Exception {
		JSONObject json = connectorWithJDBC.getJsonObject(util.getConnection(), 3);
		System.out.println(json);
	}

	@Test
	public void testIfResultSetTrue() throws IOException, SQLException {
		when(resultSet.next()).thenReturn(true);
		boolean actual = connectorWithJDBC.nextRecord();
		assertEquals(true, actual);
	}

	@Test
	public void testIfResultSetFalse() throws IOException, SQLException {
		when(resultSet.next()).thenReturn(false);
		boolean actual = connectorWithJDBC.nextRecord();
		assertEquals(false, actual);
	}

	@Test
	public void testWithGivenId() throws Exception {
		String expected = "{\n" + "\"userDetails\":\n"
				+ "[{\"req_id\":\"2\",\"country\":\"India\",\"Table Name\":\"request\",\"request_from\":\"Bangalore\",\"amount\":\"2000\",\"user_id\":\"2\",\"DataBase Name\":\"demo\"}]\n"
				+ "}";
		JSONAssert.assertEquals(expected, connectorWithJDBC.getJsonObject(util.getConnection(), 2).toString(),
				false);
	}

	@Test
	public void testForAllRecord() throws Exception {
		JSONObject allDbNames = connectorWithJDBC.getJsonObject(util.getConnection(), 0);
		JSONArray jsonArray = (JSONArray) allDbNames.get("userDetails");
		List<String> userList = new ArrayList<String>();
		for (int i = 0; i < jsonArray.size(); i++) {
			userList.add(jsonArray.get(i).toString());
		}
		Assert.assertEquals("list size", 5, userList.size());
	}

	
	@Test
	public void testForOneRecord() throws Exception {
		JSONObject allDbNames = connectorWithJDBC.getJsonObject(util.getConnection(), 1);
		JSONArray jsonArray = (JSONArray) allDbNames.get("userDetails");
		List<String> userList = new ArrayList<String>();
		for (int i = 0; i < jsonArray.size(); i++) {
			userList.add(jsonArray.get(i).toString());
		}
		Assert.assertEquals("list size", 1, userList.size());
	}

	@Test
	public void testClose() throws SQLException {
		doNothing().when(resultSet).close();
		connectorWithJDBC.close();
		verify(resultSet, times(1)).close();
	}

}
