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
package co.nubetech.hiho.mapreduce.sf;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.sf.BatchRequest;
import co.nubetech.hiho.common.sf.SFRestConnection;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;

import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.parser.PullParserException;
import com.sforce.ws.transport.JdkHttpTransport;
import com.sforce.soap.partner.PartnerConnection;

public class SalesForceLoadMapper<K, V> extends
		Mapper<LongWritable, Text, NullWritable, NullWritable> {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.sf.SalesForceLoadMapper.class);

	private SFRestConnection connection;
	private JobInfo job;
	private List<BatchInfo> batchInfos;
	private int currentBytes;
	private int currentLines;
	private int maxBytesPerBatch; // 10 million bytes per batch
	private int maxRowsPerBatch; // 10 thousand rows per batch
	private OutputStream sfOutputStream;
	boolean isDirty;
	private int numberOfBatches;

	@Override
	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {

		String userName = context.getConfiguration().get(
				HIHOConf.SALESFORCE_USERNAME);
		String password = context.getConfiguration().get(
				HIHOConf.SALESFORCE_PASSWORD);
		String sobjectType = context.getConfiguration().get(
				HIHOConf.SALESFORCE_SOBJECTYPE);
		isDirty = false;

		// System.out.println("\n inside mapper the confs are "+userName+" "+password+" "+sobjectType);

		try {

			connection = getRestConnection(userName, password);
			job = createJob(sobjectType, connection);
			logger.debug("job created and job id is:" + job.getId());

		} catch (ConnectionException e) {
			e.printStackTrace();

		} catch (AsyncApiException e) {

			e.printStackTrace();
		}

		batchInfos = new ArrayList<BatchInfo>();
		maxRowsPerBatch = 10000;
		maxBytesPerBatch = 10000000;
		currentLines = 0;
		currentBytes = 0;
		numberOfBatches = 0;

	}

	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

		String headers = context.getConfiguration().get(
				HIHOConf.SALESFORCE_HEADERS);

		byte[] sfh = (headers + "\n").getBytes();
		byte[] sfval = (val + "\n").getBytes();

		if (currentLines == 0) {
			currentBytes = sfval.length + sfh.length;
			logger.debug("MaxNumberOfBytes:" + maxBytesPerBatch
					+ " MaxNumberOfLines:" + maxRowsPerBatch
					+ " CurrentNoOfLines:" + currentLines + " CurrentNo.Bytes:"
					+ currentBytes + " NumberOfBatches:" + numberOfBatches);
		} else {
			currentBytes = sfval.length + currentBytes;
			logger.debug("MaxNumberOfBytes:" + maxBytesPerBatch
					+ " MaxNumberOfLines:" + maxRowsPerBatch
					+ " CurrentNoOfLines:" + currentLines + " CurrentNo.Bytes:"
					+ currentBytes + " NumberOfBatches:" + numberOfBatches);
		}
		isDirty = true;

		try {
			if (currentLines == 0) {
				sfOutputStream = connection.openSFStream(job);

				sfOutputStream.write(sfh);
				sfOutputStream.write(sfval);

				currentLines = 2;
				logger.debug("MaxNumberOfBytes:" + maxBytesPerBatch
						+ " MaxNumberOfLines:" + maxRowsPerBatch
						+ " CurrentNoOfLines:" + currentLines
						+ " CurrentNo.Bytes:" + currentBytes
						+ " NumberOfBatches:" + numberOfBatches);

			} else if (currentBytes < maxBytesPerBatch
					&& currentLines < maxRowsPerBatch) {
				sfOutputStream.write(sfval);
				currentLines++;
				logger.debug("MaxNumberOfBytes:" + maxBytesPerBatch
						+ " MaxNumberOfLines:" + maxRowsPerBatch
						+ " CurrentNoOfLines:" + currentLines
						+ " CurrentNo.Bytes:" + currentBytes
						+ " NumberOfBatches:" + numberOfBatches);

			} else if (currentBytes >= maxBytesPerBatch
					|| currentLines >= maxRowsPerBatch) {
				// first send off the current batch

				sfOutputStream.close();
				InputStream result = connection.getTransport().getContent();

				BatchInfo info = BatchRequest.loadBatchInfo(result);
				numberOfBatches++;
				result.close();
				batchInfos.add(info);
				logger.debug("MaxNumberOfBytes:" + maxBytesPerBatch
						+ " MaxNumberOfLines:" + maxRowsPerBatch
						+ " CurrentNoOfLines:" + currentLines
						+ " CurrentNo.Bytes:" + currentBytes
						+ " NumberOfBatches:" + numberOfBatches);

				sfOutputStream = connection.openSFStream(job);
				sfOutputStream.write(sfh);
				sfOutputStream.write(sfval);
				currentBytes = sfval.length + sfh.length;
				currentLines = 2;
				logger.debug("MaxNumberOfBytes:" + maxBytesPerBatch
						+ " MaxNumberOfLines:" + maxRowsPerBatch
						+ " CurrentNoOfLines:" + currentLines
						+ " CurrentNo.Bytes:" + currentBytes
						+ " NumberOfBatches:" + numberOfBatches);

			}
		} catch (AsyncApiException e) {
			logger.debug("AsyncApiException: " + e.getMessage());
		} catch (PullParserException e) {
			logger.debug("PullParserException: " + e.getMessage());
		} catch (ConnectionException e) {
			logger.debug("ConnectionException: " + e.getMessage());
		}
	}

	@Override
	protected void cleanup(Mapper.Context context) throws IOException,
			InterruptedException {
		try {
			if (sfOutputStream != null && isDirty) {
				sfOutputStream.close();
				InputStream result = connection.getTransport().getContent();
				BatchInfo info = BatchRequest.loadBatchInfo(result);
				numberOfBatches++;
				result.close();
				batchInfos.add(info);
			}
			logger.debug("MaxNumberOfBytes:" + maxBytesPerBatch
					+ " MaxNumberOfLines:" + maxRowsPerBatch
					+ " CurrentNoOfLines:" + currentLines + " CurrentNo.Bytes:"
					+ currentBytes + " NumberOfBatches:" + numberOfBatches);
			isDirty = false;
			closeJob(connection, job.getId());
			awaitCompletion(connection, job, batchInfos);
			checkResults(connection, job, batchInfos);

		} catch (AsyncApiException e) {
			logger.debug("AsyncApiException: " + e.getMessage());

		} catch (IOException e) {
			logger.debug("IOException: " + e.getMessage());

		} catch (PullParserException e) {
			logger.debug("PullParserException: " + e.getMessage());

		} catch (ConnectionException e) {
			logger.debug("ConnectionException: " + e.getMessage());

		}

	}

	private JobInfo createJob(String sobjectType, SFRestConnection connection)
			throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setObject(sobjectType);
		job.setOperation(OperationEnum.insert);
		job.setContentType(ContentType.CSV);
		job = connection.createJob(job);
		logger.debug(job);
		return job;
	}

	private SFRestConnection getRestConnection(String userName, String password)
			throws ConnectionException, AsyncApiException {
		ConnectorConfig partnerConfig = new ConnectorConfig();
		partnerConfig.setUsername(userName);
		partnerConfig.setPassword(password);
		partnerConfig
				.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/20.0");
		// Creating the connection automatically handles login and stores
		// the session in partnerConfig
		new PartnerConnection(partnerConfig);
		// When PartnerConnection is instantiated, a login is implicitly
		// executed and, if successful,
		// a valid session is stored in the ConnectorConfig instance.
		// Use this key to initialize a RestConnection:
		ConnectorConfig config = new ConnectorConfig();
		config.setSessionId(partnerConfig.getSessionId());
		// The endpoint for the Bulk API service is the same as for the normal
		// SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
		String soapEndpoint = partnerConfig.getServiceEndpoint();
		String apiVersion = "20.0";
		String restEndpoint = soapEndpoint.substring(0,
				soapEndpoint.indexOf("Soap/"))
				+ "async/" + apiVersion;
		config.setRestEndpoint(restEndpoint);
		// This should only be false when doing debugging.
		config.setCompression(true);
		// Set this to true to see HTTP requests and responses on stdout
		config.setTraceMessage(false);
		SFRestConnection connection = new SFRestConnection(config);
		return connection;
	}

	private void checkResults(SFRestConnection connection, JobInfo job,
			List<BatchInfo> batchInfoList) throws AsyncApiException,
			IOException {
		// batchInfoList was populated when batches were created and submitted
		for (BatchInfo b : batchInfoList) {
			CSVReader rdr = new CSVReader(connection.getBatchResultStream(
					job.getId(), b.getId()));
			List<String> resultHeader = rdr.nextRecord();
			int resultCols = resultHeader.size();

			List<String> row;
			while ((row = rdr.nextRecord()) != null) {
				Map<String, String> resultInfo = new HashMap<String, String>();
				for (int i = 0; i < resultCols; i++) {
					resultInfo.put(resultHeader.get(i), row.get(i));
				}
				boolean success = Boolean.valueOf(resultInfo.get("Success"));
				boolean created = Boolean.valueOf(resultInfo.get("Created"));
				String id = resultInfo.get("Id");
				String error = resultInfo.get("Error");
				if (success && created) {

					logger.debug("Created row with id " + id);
				} else if (!success) {

					logger.debug("Failed with error: " + error);
				}
			}
		}
	}

	private void closeJob(SFRestConnection connection, String jobId)
			throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setId(jobId);
		job.setState(JobStateEnum.Closed);
		connection.updateJob(job);
	}

	private void awaitCompletion(SFRestConnection connection, JobInfo job,
			List<BatchInfo> batchInfoList) throws AsyncApiException {
		long sleepTime = 0L;
		Set<String> incomplete = new HashSet<String>();
		for (BatchInfo bi : batchInfoList) {
			incomplete.add(bi.getId());
		}
		while (!incomplete.isEmpty()) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
			}

			logger.debug("Awaiting results..." + incomplete.size());
			sleepTime = 10000L;
			BatchInfo[] statusList = connection.getBatchInfoList(job.getId())
					.getBatchInfo();
			for (BatchInfo b : statusList) {
				if (b.getState() == BatchStateEnum.Completed
						|| b.getState() == BatchStateEnum.Failed) {
					if (incomplete.remove(b.getId())) {
						logger.debug("BATCH STATUS:\n" + b);

					}
				}
			}
		}
	}

}
