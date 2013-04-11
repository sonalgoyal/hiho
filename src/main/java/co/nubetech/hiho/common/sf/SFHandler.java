package co.nubetech.hiho.common.sf;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class SFHandler {

	/**
	 * Create the BulkConnection used to call Bulk API operations.
	 */
	public BulkConnection getBulkConnection(String userName,
			String password) throws ConnectionException, AsyncApiException {
		ConnectorConfig partnerConfig = new ConnectorConfig();
		partnerConfig.setUsername(userName);
		partnerConfig.setPassword(password);
		partnerConfig
				.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/27.0");
		// Creating the connection automatically handles login and stores
		// the session in partnerConfig
		new PartnerConnection(partnerConfig);
		// When PartnerConnection is instantiated, a login is implicitly
		// executed and, if successful,
		// a valid session is stored in the ConnectorConfig instance.
		// Use this key to initialize a BulkConnection:
		ConnectorConfig config = new ConnectorConfig();
		config.setSessionId(partnerConfig.getSessionId());
		// The endpoint for the Bulk API service is the same as for the normal
		// SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
		String soapEndpoint = partnerConfig.getServiceEndpoint();
		String apiVersion = "27.0";
		String restEndpoint = soapEndpoint.substring(0,
				soapEndpoint.indexOf("Soap/"))
				+ "async/" + apiVersion;
		config.setRestEndpoint(restEndpoint);
		// This should only be false when doing debugging.
		config.setCompression(true);
		// Set this to true to see HTTP requests and responses on stdout
		config.setTraceMessage(false);
		BulkConnection connection = new BulkConnection(config);
		return connection;
	}

	/**
	 * Create a new job using the Bulk API.
	 * 
	 * @param sobjectType
	 * 
	 *            The object type being loaded, such as "Account"
	 * @param connection
	 * 
	 *            BulkConnection used to create the new job.
	 * @return The JobInfo for the new job.
	 * @throws AsyncApiException
	 */
	public JobInfo createJob(String sobjectType,
			BulkConnection connection) throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setObject(sobjectType);
		job.setOperation(OperationEnum.insert);
		job.setContentType(ContentType.CSV);
		job = connection.createJob(job);
		System.out.println(job);
		return job;
	}

	public void closeJob(BulkConnection connection, String jobId)
			throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setId(jobId);
		job.setState(JobStateEnum.Closed);
		connection.updateJob(job);
	}

	/**
	 * Wait for a job to complete by polling the Bulk API.
	 * 
	 * @param connection
	 * 
	 *            BulkConnection used to check results.
	 * @param job
	 * 
	 *            The job awaiting completion.
	 * @param batchInfoList
	 * 
	 *            List of batches for this job.
	 * @throws AsyncApiException
	 */
	public void awaitCompletion(BulkConnection connection, JobInfo job,
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
			System.out.println("Awaiting results..." + incomplete.size());
			sleepTime = 10000L;
			BatchInfo[] statusList = connection.getBatchInfoList(job.getId())
					.getBatchInfo();
			for (BatchInfo b : statusList) {
				if (b.getState() == BatchStateEnum.Completed
						|| b.getState() == BatchStateEnum.Failed) {
					if (incomplete.remove(b.getId())) {
						System.out.println("BATCH STATUS:\n" + b);
					}
				}
			}
		}
	}

	/**
	 * Gets the results of the operation and checks for errors.
	 */
	public void checkResults(BulkConnection connection, JobInfo job,
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
					System.out.println("Created row with id " + id);
				} else if (!success) {
					System.out.println("Failed with error: " + error);
				}
			}
		}
	}
	
	
	/**
	 * Create a batch by uploading the contents of the file. This closes the
	 * output stream.
	 * 
	 * @param tmpOut
	 * 
	 *            The output stream used to write the CSV data for a single
	 *            batch.
	 * @param tmpFile
	 * 
	 *            The file associated with the above stream.
	 * @param batchInfos
	 * 
	 *            The batch info for the newly created batch is added to this
	 *            list.
	 * @param connection
	 * 
	 *            The BulkConnection used to create the new batch.
	 * @param jobInfo
	 * 
	 *            The JobInfo associated with the new batch.
	 */
	public BatchInfo createBatch(FSDataInputStream tmpInputStream,
			BulkConnection connection,
			JobInfo jobInfo) throws IOException, AsyncApiException {
		try {
			BatchInfo batchInfo = connection.createBatchFromStream(jobInfo,
					tmpInputStream);
			System.out.println(batchInfo);
			return batchInfo;
		} finally {
			tmpInputStream.close();
		}
	}

}
