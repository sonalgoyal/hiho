package co.nubetech.hiho.mapreduce.sf;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.sf.SFHandler;

import com.sforce.async.BatchInfo;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;

/**
 * Create connection and job in setup
 * create batches in mapper
 * emit jobid and batchinfo, let the reducer handle status
 * @author sgoyal
 *
 */

public class ExportSalesforceMapper extends Mapper<Text, FSDataInputStream, Text, Text> {

		final static Logger logger = Logger
				.getLogger(co.nubetech.hiho.mapreduce.sf.ExportSalesforceMapper.class);
		private static SFHandler sfHandler;
		private BulkConnection conn;
		private JobInfo job;
		private Text jobId;
		private Text batchId;
		
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
			ExportSalesforceMapper.sfHandler = sfHandler;
		}

		public void setConnection(BulkConnection con) throws IOException{
			conn=con;
		}
		
		public BulkConnection getConnection(){
			return conn;			
		}
		
		@Override
		protected void setup(Mapper.Context context) throws IOException,
				InterruptedException {
			try {
				String username = context.getConfiguration().get(
						HIHOConf.SALESFORCE_USERNAME);
				String password = context.getConfiguration().get(
						HIHOConf.SALESFORCE_PASSWORD);
				String sfObject = context.getConfiguration().get(
						HIHOConf.SALESFORCE_SOBJECTYPE);
				logger.debug("Salesforce connection values are " + username
						+ "/" + password + " and sObject is " + sfObject);
				conn = sfHandler.getBulkConnection(username, password);
				job = sfHandler.createJob(sfObject, conn);
				jobId = new Text(job.getId());
				batchId = new Text();
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException(e);
			}

		}

		
		@Override
		public void map(Text key, FSDataInputStream val, Context context)
				throws IOException, InterruptedException {
	       try{   
				conn=getConnection();
				BatchInfo batch = sfHandler.createBatch(val, conn, job);
				batchId.set(batch.getId());
				context.write(jobId, batchId);
	       }
	       catch(Exception e) {
	    	   e.printStackTrace();
	    	   throw new IOException(e);
	       }
			
		}

		/**
		 * @return the jobId
		 */
		public Text getJobId() {
			return jobId;
		}

		/**
		 * @param jobId the jobId to set
		 */
		public void setJobId(Text jobId) {
			this.jobId = jobId;
		}

		/**
		 * @return the batchId
		 */
		public Text getBatchId() {
			return batchId;
		}

		/**
		 * @param batchId the batchId to set
		 */
		public void setBatchId(Text batchId) {
			this.batchId = batchId;
		}

		@Override
		protected void cleanup(Mapper.Context context) throws IOException,
				InterruptedException {
			try {
				if (conn != null) {
					sfHandler.closeJob(conn, job.getId());
				}
			} catch (Exception s) {
				s.printStackTrace();
			}
		}

	}
