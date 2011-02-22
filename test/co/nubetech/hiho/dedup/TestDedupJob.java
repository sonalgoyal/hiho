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
package co.nubetech.hiho.dedup;

import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;

public class TestDedupJob {

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-inputPath",
				"testData/dedup/inputForSeqTest", "-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(args);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputFormat()
			throws HIHOException {
		String[] arg = new String[] { "-dedupBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-inputPath",
				"testData/dedup/inputForSeqTest", "-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInDedupBy()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-inputPath", "testData/dedup/inputForSeqTest", "-outputPath",
				"output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputKeyClassName()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-inputPath",
				"testData/dedup/inputForSeqTest", "-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputValueClassName()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputPath",
				"testData/dedup/inputForSeqTest", "-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputPath()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInOutputPath()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-inputPath",
				"testData/dedup/inputForSeqTest" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}
}
