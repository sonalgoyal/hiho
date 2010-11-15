/**
 * Copyright 2010 Nube Technologies
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License. 
 */
package co.nubetech.hiho.common.sf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;

import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.RestConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.parser.PullParserException;
import com.sforce.ws.parser.XmlInputStream;
import com.sforce.ws.transport.JdkHttpTransport;
import com.sforce.ws.util.FileUtil;

public class SFRestConnection extends RestConnection {

	private ConnectorConfig config;
	private HashMap<String, String> headers = new HashMap<String, String>();

	public SFRestConnection(ConnectorConfig config) throws AsyncApiException {
		super(config);

		if (config == null) {
			throw new AsyncApiException("config can not be null",
					AsyncExceptionCode.ClientInputError);
		}

		if (config.getRestEndpoint() == null) {
			throw new AsyncApiException("rest endpoint cannot be null",
					AsyncExceptionCode.ClientInputError);
		}

		this.config = config;

		if (config.getSessionId() == null) {
			throw new AsyncApiException("session ID not found",
					AsyncExceptionCode.ClientInputError);
		}

	}

	private JdkHttpTransport transport;

	public JdkHttpTransport getTransport() {
		return transport;
	}

	public void setTransport(JdkHttpTransport transport) {
		this.transport = transport;
	}

	public OutputStream openSFStream(JobInfo jobInfo) throws AsyncApiException {
		try {
			String endpoint = getRestEndpoint();
			transport = new JdkHttpTransport(config);
			endpoint = endpoint + "job/" + jobInfo.getId() + "/batch";
			String contentType = jobInfo.getContentType() == ContentType.CSV ? CSV_CONTENT_TYPE
					: XML_CONTENT_TYPE;
			OutputStream out = transport.connect(endpoint,
					getHeaders(contentType));
			return out;
		} catch (IOException e) {
			throw new AsyncApiException("Failed to create batch",
					AsyncExceptionCode.ClientInputError, e);
		}
	}

	/*
	 * public OutputStream appendBatchFromStream (JobInfo jobInfo, InputStream
	 * input) throws AsyncApiException { try { String endpoint =
	 * getRestEndpoint(); JdkHttpTransport transport = new
	 * JdkHttpTransport(config); endpoint = endpoint + "job/" + jobInfo.getId()
	 * + "/batch"; String contentType = jobInfo.getContentType() ==
	 * ContentType.CSV ? CSV_CONTENT_TYPE : XML_CONTENT_TYPE; OutputStream out =
	 * transport.connect(endpoint, getHeaders(contentType)); return out; } catch
	 * (IOException e) { throw new AsyncApiException("Failed to create batch",
	 * AsyncExceptionCode.ClientInputError, e); } }
	 */

	static void parseAndThrowException(InputStream in) throws AsyncApiException {
		try {
			XmlInputStream xin = new XmlInputStream();
			xin.setInput(in, "UTF-8");
			AsyncApiException exception = new AsyncApiException();
			exception.load(xin, typeMapper);
			throw exception;
		} catch (PullParserException e) {
			throw new AsyncApiException("Failed to parse exception ",
					AsyncExceptionCode.ClientInputError, e);
		} catch (IOException e) {
			throw new AsyncApiException("Failed to parse exception",
					AsyncExceptionCode.ClientInputError, e);
		} catch (ConnectionException e) {
			throw new AsyncApiException("Failed to parse exception ",
					AsyncExceptionCode.ClientInputError, e);
		}
	}

	private HashMap<String, String> getHeaders(String contentType) {
		HashMap<String, String> newMap = new HashMap<String, String>();

		for (Map.Entry<String, String> entry : headers.entrySet()) {
			newMap.put(entry.getKey(), entry.getValue());
		}

		newMap.put("Content-Type", contentType);
		newMap.put(SESSION_ID, config.getSessionId());
		return newMap;
	}

	private String getRestEndpoint() {
		String endpoint = config.getRestEndpoint();
		endpoint = endpoint.endsWith("/") ? endpoint : endpoint + "/";
		return endpoint;
	}

}
