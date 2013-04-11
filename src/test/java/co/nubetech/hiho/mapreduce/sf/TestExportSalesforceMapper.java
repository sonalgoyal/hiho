
package co.nubetech.hiho.mapreduce.sf;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.sf.SFHandler;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.ws.ConnectionException;

public class TestExportSalesforceMapper {
	
	@Test
	public final void testMapperForNullValues() throws IOException, InterruptedException, ConnectionException, AsyncApiException {
		Mapper.Context context = mock(Mapper.Context.class);
		SFHandler mockSFHandler = mock(SFHandler.class);
		when(mockSFHandler.getBulkConnection(anyString(), anyString())).thenReturn(null);
		BatchInfo batchInfo = new BatchInfo();
		String id = "id";
		batchInfo.setId(id);
		when(mockSFHandler.createBatch(any(FSDataInputStream.class), any(BulkConnection.class), any(JobInfo.class))).thenReturn(
				batchInfo);
		ExportSalesforceMapper mapper = new ExportSalesforceMapper();
		mapper.setSfHandler(mockSFHandler);
		Text batchId = new Text(id);
		mapper.setBatchId(batchId);
		mapper.map(null, null, context);	
	}
	
	@Test
	public final void testSetup() throws IOException,
		InterruptedException, ConnectionException, AsyncApiException {
		Mapper.Context context = mock(Mapper.Context.class);
		Configuration conf = new Configuration();
		String user = "user";
		String password = "pass";
		String sObject = "Accounts";
		conf.set(HIHOConf.SALESFORCE_USERNAME, user);
		conf.set(HIHOConf.SALESFORCE_PASSWORD, password);
		conf.set(HIHOConf.SALESFORCE_SOBJECTYPE, sObject);
		when(context.getConfiguration()).thenReturn(conf);
		
		SFHandler mockSFHandler = mock(SFHandler.class);
		when(mockSFHandler.getBulkConnection(user, password)).thenReturn(null);
		
		JobInfo info = new JobInfo();
		String id = "id";
		info.setId(id);
		when(mockSFHandler.createJob(sObject, null)).thenReturn(info);
		
		ExportSalesforceMapper mapper = new ExportSalesforceMapper();
		mapper.setSfHandler(mockSFHandler);
		mapper.setup(context);
		verify(context, times(3)).getConfiguration();	
		verify(mockSFHandler, times(1)).getBulkConnection(user, password);	
		assertEquals(id, mapper.getJobId().toString());
	}
	
	/*
	 * conn=getConnection();
				BatchInfo batch = sfHandler.createBatch(val, conn, job);
				batchId.set(batch.getId());
				context.write(jobId, batchId);
	 */

	@Test
	public final void testMapper() throws IOException, InterruptedException, ConnectionException, AsyncApiException {
		ExportSalesforceMapper mapper = new ExportSalesforceMapper();
		BulkConnection conn = mock(BulkConnection.class);
		SFHandler mockSFHandler = mock(SFHandler.class);
		mapper.setConnection(conn);
		mapper.setSfHandler(mockSFHandler);
		
		Mapper.Context context = mock(Mapper.Context.class);
		BatchInfo batchInfo = new BatchInfo();
		String id = "id";
		batchInfo.setId(id);
		Text batchId = new Text();
		mapper.setBatchId(batchId);		
		
		JobInfo info = new JobInfo();
		String id1 = "id1";
		info.setId(id1);
		when(mockSFHandler.createBatch(any(FSDataInputStream.class), any(BulkConnection.class), any(JobInfo.class))).thenReturn(
				batchInfo);
		mapper.setSfHandler(mockSFHandler);
		mapper.setJob(info);
		mapper.setJobId(new Text(id1));
		mapper.map(new Text("abc"), mock(FSDataInputStream.class), context);
		assertEquals(id, batchId.toString());
		verify(context, times(1)).write(new Text(info.getId()), batchId);		
	}

}
