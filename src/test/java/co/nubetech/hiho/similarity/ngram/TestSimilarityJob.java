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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.HihoTestCase;

public class TestSimilarityJob extends HihoTestCase {
	
	final static Logger logger = Logger
	.getLogger(co.nubetech.hiho.similarity.ngram.TestSimilarityJob.class);
	
	
	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] {
				"-inputPath", "testData",
				};
		SimilarityJob similarityJob = new SimilarityJob();
		similarityJob.populateConfiguration(args);
		similarityJob.checkMandatoryConfs();
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputPath()
			throws HIHOException {
		String[] args = new String[] {};
		SimilarityJob similarityJob = new SimilarityJob();
		similarityJob.populateConfiguration(args);
		similarityJob.checkMandatoryConfs();
	}
	
	
	//Not working, as SimilarityJob is finding file on local not on cluster.
	@Test
	public void testSimilarityJobForValidValues() throws Exception{
		final String inputData = "This is a book	value1\nThis is not a book	value2";
		createTextFileInHDFS(inputData, "input", "testFile1.txt");	
		String[] args = new String[] {
				"-inputPath", "input"
				};
		SimilarityJob.main(args);	
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "outputOfScoreJob/part-r-00000");
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
		Writable writableKey = (Writable)
		ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable writableValue = (Writable)
		ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		List<ValuePair> expectedOutputForKey = new ArrayList<ValuePair>();
		ValuePair valuePair = new ValuePair();
		valuePair.setValue1(new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		valuePair.setValue2(new Text("This is not a bookdelimiterBetweenKeyAndValuevalue2"));
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
