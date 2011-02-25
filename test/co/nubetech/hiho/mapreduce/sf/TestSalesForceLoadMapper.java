package co.nubetech.hiho.mapreduce.sf;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.JobInfo;

import co.nubetech.hiho.common.sf.SFRestConnection;

public class TestSalesForceLoadMapper {

	@Test
	public void testCheckResults() throws AsyncApiException, IOException{
		SFRestConnection connection =mock(SFRestConnection.class);
		JobInfo job=mock(JobInfo.class);
		job.setId("id0");
		BatchInfo batchInfo0=new BatchInfo();
		batchInfo0.setId("id0");
		BatchInfo batchInfo1=new BatchInfo();
		batchInfo1.setId("id1");
		List<BatchInfo> batchInfoList= new ArrayList<BatchInfo>();
		batchInfoList.add(batchInfo0);
		batchInfoList.add(batchInfo1);
		
		SalesForceLoadMapper mapper=new SalesForceLoadMapper();
		//mapper.checkResults(connection, job, batchInfoList);
	}
}
