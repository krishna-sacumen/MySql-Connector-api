package com.bigid.connector;

import java.io.UnsupportedEncodingException;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import com.google.gson.JsonObject;

public class MySqlRequest {

	private String WEB_URL = "http://localhost:8080/";
	private HttpPost postRequest;
	private HttpGet getRequest;
	private String responseBody;

	public void flush() {
		addRequestBodyToPostRequest(MySqlConfig.FLUSH_URL, "");

		executePostRequest();

	}

	public void addRequestBodyToPostRequest(String endPoint, String reqBody) {
		postRequest = new HttpPost(WEB_URL + endPoint);

		postRequest.setHeader("Content-Type", "application/json");
		postRequest.setHeader("x-access-token", "bigid_mock");
		try {
			postRequest.setEntity(new StringEntity(reqBody));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public void addHeaderToGetRequest(String endPoint) {
		getRequest = new HttpGet(WEB_URL + endPoint);

		getRequest.setHeader("Content-Type", "application/json");
		getRequest.setHeader("x-access-token", "bigid_mock");
	}

	public void executePostRequest() {
		try {

		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	public void executeGetRequest() {
		try {

		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	private void addRequestBodyToPostRequestForLogin(String endPoint, String reqBody) {
		postRequest = new HttpPost(endPoint);
		postRequest.setHeader("Content-Type", "application/json");
		try {
			postRequest.setEntity(new StringEntity(reqBody));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	private String loginRequestBody() {
		JsonObject data = new JsonObject();
		data.addProperty("username", "bigid");
		data.addProperty("password", "bigid111");

		return data.toString();

	}
	public String getResponseBody() {
		return responseBody;
	}

	public void setResponseBody(String responseBody) {
		this.responseBody = responseBody;
	}

}
