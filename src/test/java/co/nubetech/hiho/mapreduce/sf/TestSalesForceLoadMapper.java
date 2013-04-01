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

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import co.nubetech.hiho.common.sf.SFRestConnection;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.JobInfo;

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
