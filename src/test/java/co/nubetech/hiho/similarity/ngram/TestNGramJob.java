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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.HihoTestCase;

public class TestNGramJob extends HihoTestCase {
	
	final static Logger logger = Logger
	.getLogger(co.nubetech.hiho.similarity.ngram.TestNGramJob.class);
	
	
	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] {
				"-inputPath", "testData",
				};
		NGramJob nGramJob = new NGramJob();
		nGramJob.populateConfiguration(args);
		nGramJob.checkMandatoryConfs();
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputPath()
			throws HIHOException {
		String[] args = new String[] {};
		NGramJob nGramJob = new NGramJob();
		nGramJob.populateConfiguration(args);
		nGramJob.checkMandatoryConfs();
	}
	
	public NGramJob runNgramJob(String[] args) throws Exception {
		NGramJob job = new NGramJob();			
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);
		return job;
	}

	
	@Test
	public void testNGramJobForValidValues() throws Exception{
		final String inputData = "This is a book	value1\nThis is not a book	value2";
		createTextFileInHDFS(inputData, "/input", "testFile1.txt");	
		String[] args = new String[] {
				"-inputPath", "/input"
				};
		NGramJob job = runNgramJob(args);	
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "outputOfNGramJob/part-r-00000");
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
		Writable writableKey = (Writable)
		ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable writableValue = (Writable)
		ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		List<IntWritable> expectedOutputForValue = new ArrayList<IntWritable>();
		expectedOutputForValue.add(new IntWritable(1));
		expectedOutputForValue.add(new IntWritable(1));
		
		int count = 0;
		while (reader.next(writableKey, writableValue)) {
			logger.debug("Key and value is: " + writableKey + ", " + writableValue);
			assertTrue("Matched output " + writableValue , expectedOutputForValue.contains(writableValue));
			count++;
		}
		IOUtils.closeStream(reader);
		assertEquals(2, count);			
	}
	
}
