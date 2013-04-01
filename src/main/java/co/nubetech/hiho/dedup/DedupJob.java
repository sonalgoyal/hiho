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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOException;

public class DedupJob extends Configured implements Tool {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.DedupJob.class);
	private String inputFormat = null;
	private String dedupBy = null; // "key" or "value"
	private String inputKeyClassName = null;
	private String inputValueClassName = null;
	private String inputPath = null;
	private String outputPath = null;
	private String outputFormat = null;
	private String delimiter = ",";
	private int column = 1;
	
	private long totalRecordsRead;
	private long badRecords;
	private long output;
	private long duplicateRecords;

	public long getTotalRecordsRead() {
		return totalRecordsRead;
	}

	public void setTotalRecordsRead(long totalRecordsRead) {
		this.totalRecordsRead = totalRecordsRead;
	}

	public long getBadRecords() {
		return badRecords;
	}

	public void setBadRecords(long badRecords) {
		this.badRecords = badRecords;
	}

	public long getOutput() {
		return output;
	}

	public void setOutput(long output) {
		this.output = output;
	}

	public long getDuplicateRecords() {
		return duplicateRecords;
	}

	public void setDuplicateRecords(long duplicateRecords) {
		this.duplicateRecords = duplicateRecords;
	}

	public void populateConfiguration(String[] args) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-inputFormat".equals(args[i])) {
				inputFormat = args[++i];
				if (inputFormat
						.equals("org.apache.hadoop.mapreduce.lib.input.TextInputFormat")) {
					inputKeyClassName = "org.apache.hadoop.io.LongWritable";
					inputValueClassName = "org.apache.hadoop.io.Text";
				} else if (inputFormat
						.equals("co.nubetech.hiho.dedup.DelimitedTextInputFormat")) {
					inputKeyClassName = "org.apache.hadoop.io.Text";
					inputValueClassName = "org.apache.hadoop.io.Text";
					outputFormat = "co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat";
				} else if (inputFormat
						.equals("org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat")) {
					outputFormat = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat";
				}
			} else if ("-dedupBy".equals(args[i])) {
				dedupBy = args[++i];
			} else if ("-inputKeyClassName".equals(args[i])) {
				inputKeyClassName = args[++i];
			} else if ("-inputValueClassName".equals(args[i])) {
				inputValueClassName = args[++i];
			} else if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			} else if ("-outputPath".equals(args[i])) {
				outputPath = args[++i];
			} else if ("-delimiter".equals(args[i])) {
				delimiter = args[++i];
			} else if ("-column".equals(args[i])) {
				column = Integer.parseInt(args[++i]);
			} else if ("-outputFormat".equals(args[i])) {
				outputFormat = args[++i];
			}
		}
	}

	public void checkMandatoryConfs() throws HIHOException {
		if (inputFormat == null) {
			throw new HIHOException(
					"The provided input format is empty, please specify inputFormat");
		}
		if (dedupBy == null) {
			throw new HIHOException(
					"The provided value of dedupBy is empty, please specify either key or value");
		}
		if ((!dedupBy.equals("key")) && (!dedupBy.equals("value"))) {
			throw new HIHOException(
					"The provided value of dedupBy is Incorrect, please specify either key or value");
		}
		if (inputKeyClassName == null) {
			throw new HIHOException(
					"The provided input key class name is empty, please specify inputKeyClassName");
		}
		if (inputValueClassName == null) {
			throw new HIHOException(
					"The provided input value class name is empty, please specify inputValueClassName");
		}
		if (inputPath == null) {
			throw new HIHOException(
					"The provided input path is empty, please specify inputPath");
		}
		if (outputPath == null) {
			throw new HIHOException(
					"The provided output path is empty, please specify outputPath");
		}
		if (outputFormat == null) {
			System.out.println(outputFormat);
			throw new HIHOException(
					"The provided output format is empty, please specify outputFormat");
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		populateConfiguration(args);
		try {
			checkMandatoryConfs();
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new Exception(e1);
		}
		Job job = new Job(conf);
		job.setJobName("Dedup job");
		job.setJarByClass(DedupJob.class);

		Class inputFormatClass = Class.forName(inputFormat);
		Class outputFormatClass = Class.forName(outputFormat);
		Class inputKeyClass = Class.forName(inputKeyClassName);
		Class inputValueClass = Class.forName(inputValueClassName);

		if (dedupBy.equals("key")) {
			job.setMapperClass(DedupKeyMapper.class);
			job.setReducerClass(DedupKeyReducer.class);
			job.setMapOutputValueClass(inputValueClass);
		} else if (dedupBy.equals("value")) {
			job.setMapperClass(DedupValueMapper.class);
			job.setReducerClass(DedupValueReducer.class);
			job.setMapOutputValueClass(inputKeyClass);
		}

		job.setInputFormatClass(inputFormatClass);
		if (inputFormat
				.equals("co.nubetech.hiho.dedup.DelimitedTextInputFormat")) {
			DelimitedTextInputFormat.setProperties(job, delimiter, column);
		}

		job.setMapOutputKeyClass(HihoTuple.class);

		job.setOutputKeyClass(inputKeyClass);
		job.setOutputValueClass(inputValueClass);
		job.setPartitionerClass(HihoHashPartitioner.class);
		FileInputFormat.setInputPaths(job, inputPath);
		job.setOutputFormatClass(outputFormatClass);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		try {
			logger.debug("Output format class is " + job.getOutputFormatClass());
			logger.debug("Class is "
					+ ReflectionUtils
							.newInstance(job.getOutputFormatClass(),
									job.getConfiguration()).getClass()
							.getName());
			job.waitForCompletion(false);
			if (job.isComplete()) {
				Counters counters = job.getCounters();
				totalRecordsRead = counters.findCounter(
						DedupRecordCounter.TOTAL_RECORDS_READ).getValue();
				badRecords = counters.findCounter(
						DedupRecordCounter.BAD_RECORD).getValue();
				output = counters.findCounter(DedupRecordCounter.OUTPUT)
						.getValue();
				duplicateRecords = totalRecordsRead - output;
				logger.info("Total records read are: " + totalRecordsRead);
				logger.info("Bad Records are: " + badRecords);
				logger.info("Output records are: " + output);
				logger.info("Duplicate records are: "
						+ duplicateRecords);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		DedupJob job = new DedupJob();
		int res = ToolRunner.run(new Configuration(), job, args);
		System.exit(res);
	}

}
