/**
 * Copyright 2011 Nube Technologies
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
package co.nubetech.hiho.similarity.ngram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.Test;

import co.nubetech.hiho.common.HihoTestCase;

public class TestScoreJob extends HihoTestCase {
	
	final static Logger logger = Logger
	.getLogger(co.nubetech.hiho.similarity.ngram.TestScoreJob.class);
	
	public ScoreJob runScoreJob(String[] args) throws Exception {
		ScoreJob job = new ScoreJob();			
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);
		return job;
	}
	
	@Test
	public void testScoreJobForValidValues() throws Exception{
		ValuePair valuePair = new ValuePair();
		valuePair.setValue1(new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		valuePair.setValue2(new Text("This is not a bookdelimiterBetweenKeyAndValuevalue2"));
		HashMap<ValuePair, IntWritable> inputData1 = new HashMap<ValuePair, IntWritable>();
		inputData1.put(valuePair, new IntWritable(1));
		createSequenceFileInHdfs(inputData1, "outputOfNGramJob", "part-r-00000");
		
		HashMap<ValuePair, IntWritable> inputData2 = new HashMap<ValuePair, IntWritable>();
		inputData2.put(valuePair, new IntWritable(1));
		createSequenceFileInHdfs(inputData2, "outputOfNGramJob", "part-r-00001");
		
		
		String[] args = new String[] {};
		ScoreJob job = runScoreJob(args);	
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "outputOfScoreJob/part-r-00000");
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
		Writable writableKey = (Writable)
		ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable writableValue = (Writable)
		ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		List<ValuePair> expectedOutputForKey = new ArrayList<ValuePair>();
		expectedOutputForKey.add(valuePair);
		
		List<LongWritable> expectedOutputForValue = new ArrayList<LongWritable>();
		expectedOutputForValue.add(new LongWritable(2));
		
		int count = 0;
		while (reader.next(writableKey, writableValue)) {
			logger.debug("Key and value is: " + writableKey + ", " + writableValue);
			assertTrue("Matched output " + writableKey , expectedOutputForKey.contains(writableKey));
			assertTrue("Matched output " + writableValue , expectedOutputForValue.contains(writableValue));
			count++;
		}
		IOUtils.closeStream(reader);
		assertEquals(1, count);			
	}

}
