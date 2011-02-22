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
