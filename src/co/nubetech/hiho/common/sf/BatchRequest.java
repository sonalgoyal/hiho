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

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.AsyncXmlOutputStream;
import com.sforce.async.BatchInfo;
import com.sforce.async.RestConnection;
import com.sforce.async.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.parser.PullParserException;
import com.sforce.ws.parser.XmlInputStream;
import com.sforce.ws.parser.XmlOutputStream;
import com.sforce.ws.transport.JdkHttpTransport;
import com.sforce.ws.wsdl.Constants;

public class BatchRequest {

	private XmlOutputStream xmlStream;
	private JdkHttpTransport transport;

	public BatchRequest(JdkHttpTransport transport, OutputStream out)
			throws IOException {

		this.transport = transport;

		xmlStream = new AsyncXmlOutputStream(out, false);
		xmlStream.setPrefix("xsi", Constants.SCHEMA_INSTANCE_NS);
		xmlStream.writeStartTag(RestConnection.NAMESPACE, "sObjects");
	}

	public void addSObject(SObject object) throws AsyncApiException {
		try {

			object.write(xmlStream);

		} catch (IOException e) {
			throw new AsyncApiException("Failed to add SObject",
					AsyncExceptionCode.ClientInputError, e);
		}

	}

	public void addSObjects(SObject[] objects) throws AsyncApiException {
		for (SObject object : objects) {

			addSObject(object);
		}
	}

	public BatchInfo completeRequest() throws AsyncApiException {
		try {
			xmlStream.writeEndTag(RestConnection.NAMESPACE, "sObjects");
			xmlStream.endDocument();
			xmlStream.close();
			InputStream in = transport.getContent();

			if (transport.isSuccessful()) {
				return loadBatchInfo(in);
			} else {
				SFRestConnection.parseAndThrowException(in);
			}

		} catch (IOException e) {
			throw new AsyncApiException("Failed to complete request",
					AsyncExceptionCode.ClientInputError, e);
		} catch (PullParserException e) {
			throw new AsyncApiException("Failed to complete request",
					AsyncExceptionCode.ClientInputError, e);
		} catch (ConnectionException e) {
			throw new AsyncApiException("Failed to complete request",
					AsyncExceptionCode.ClientInputError, e);
		}
		return null;
	}

	public static BatchInfo loadBatchInfo(InputStream in)
			throws PullParserException, IOException, ConnectionException {

		BatchInfo info = new BatchInfo();
		XmlInputStream xin = new XmlInputStream();
		xin.setInput(in, "UTF-8");
		info.load(xin, RestConnection.typeMapper);
		return info;

	}
}
