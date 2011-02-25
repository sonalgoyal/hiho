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
package co.nubetech.hiho.merge;

import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;

public class TestMergeJob {

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-oldPath",
				"testData/dedup/inputForSeqTest/old", "-newPath",
				"testData/dedup/inputForSeqTest/new", "-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputFormat()
			throws HIHOException {
		String[] args = new String[] { "-mergeBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-oldPath",
				"testData/dedup/inputForSeqTest/old", "-newPath",
				"testData/dedup/inputForSeqTest/new", "-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInMergeBy()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-oldPath", "testData/dedup/inputForSeqTest/old", "-newPath",
				"testData/dedup/inputForSeqTest/new", "-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputKeyClassName()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-oldPath",
				"testData/dedup/inputForSeqTest/old", "-newPath",
				"testData/dedup/inputForSeqTest/new", "-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputValueClassName()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-oldPath",
				"testData/dedup/inputForSeqTest/old", "-newPath",
				"testData/dedup/inputForSeqTest/new", "-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInOldPath()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-newPath",
				"testData/dedup/inputForSeqTest/new", "-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInNewPath()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-oldPath",
				"testData/dedup/inputForSeqTest/old", "-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInOutputPath()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key", "-inputKeyClassName",
				"org.apache.hadoop.io.IntWritable", "-inputValueClassName",
				"org.apache.hadoop.io.Text", "-oldPath",
				"testData/dedup/inputForSeqTest/old", "-newPath",
				"testData/dedup/inputForSeqTest/new" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}
}
