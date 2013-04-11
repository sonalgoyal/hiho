package co.nubetech.hiho.mapreduce.sf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.sf.SFHandler;

import com.sforce.async.BatchResult;
import com.sforce.async.BulkConnection;

public class ExportSalesforceReducer extends Reducer<Text, Text, Text, Text> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.sf.ExportSalesforceReducer.class);
	private static SFHandler sfHandler;
	private BulkConnection conn;
	private Text batchResultText;

	/**
	 * @return the sfHandler
	 */
	public static SFHandler getSfHandler() {
		return sfHandler;
	}

	/**
	 * @param sfHandler the sfHandler to set
	 */
	public static void setSfHandler(SFHandler sfHandler) {
		ExportSalesforceReducer.sfHandler = sfHandler;
	}
	
	public void setConnection(BulkConnection con) throws IOException {
		conn = con;
	}

	public BulkConnection getConnection() {
		return conn;
	}

	@Override
	protected void setup(Reducer.Context context) throws IOException,
			InterruptedException {
		try {
			String username = context.getConfiguration().get(
					HIHOConf.SALESFORCE_USERNAME);
			String password = context.getConfiguration().get(
					HIHOConf.SALESFORCE_PASSWORD);
			String sfObject = context.getConfiguration().get(
					HIHOConf.SALESFORCE_SOBJECTYPE);
			logger.debug("Salesforce connection values are " + username + "/"
					+ password + " and sObject is " + sfObject);
			conn = sfHandler.getBulkConnection(username, password);
			batchResultText = new Text();
			sfHandler = new SFHandler();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	
	@Override
	public void reduce(Text jobId, Iterable<Text> batches, Context context)
			throws IOException, InterruptedException {
       try{   
			conn=getConnection();
			Iterator<Text> batchIterator = batches.iterator();
			String jobIdString = jobId.toString();
			jobId.set(jobIdString);
			while (batchIterator.hasNext()) {
				BatchResult result = conn.getBatchResult(jobIdString, batchIterator.next().toString());
				batchResultText.set(result.toString());
				logger.debug("Job Id " + jobIdString + " and Batch " + result);
				context.write(jobId, batchResultText);
			}			
       }
       catch(Exception e) {
    	   e.printStackTrace();
    	   throw new IOException(e);
       }		
	}


	@Override
	protected void cleanup(Reducer.Context context) throws IOException,
			InterruptedException {
		try {
			if (conn != null) {
				//sfHandler.closeJob(conn, job.getId());
				conn = null;
			}
		} catch (Exception s) {
			s.printStackTrace();
		}
	}

}
