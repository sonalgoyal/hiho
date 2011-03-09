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
package co.nubetech.hiho.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class TestFileStreamInputFormat {
	
	 private MiniDFSCluster newDFSCluster(JobConf conf) throws Exception {
		    return new MiniDFSCluster(conf, 4, true,
		                         new String[]{"/rack0", "/rack0",
		                                      "/rack1", "/rack1"},
		                         new String[]{"host0", "host1",
		                                      "host2", "host3"});
		  }
	 @Test
	 public void testNumInputs() throws Exception {
		 Configuration conf = new Configuration();
		 JobConf job = new JobConf(conf);
		 MiniDFSCluster dfs = newDFSCluster(job);
		    FileSystem fs = dfs.getFileSystem();
		    System.out.println("FileSystem " + fs.getUri());
		    Path inputDir = new Path("/foo/");
		    final int numFiles = 10;
		    String fileNameBase = "part-0000";
		 
	 }
}
