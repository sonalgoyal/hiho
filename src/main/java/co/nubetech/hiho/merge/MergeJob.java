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

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.dedup.DelimitedTextInputFormat;
import co.nubetech.hiho.dedup.HihoTuple;

public class MergeJob extends Configured implements Tool {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.merge.MergeJob.class);

	private String oldPath = null;
	private String newPath = null;
	private String mergeBy = null;
	private String delimiter = ",";
	private int column = 1;
	private String inputFormat = null;
	private String inputKeyClassName = null;
	private String inputValueClassName = null;
	private String outputPath = null;
	private String outputFormat = null;

	private long output;
	private long badRecords;
	private long totalRecordsNew;
	private long totalRecordsOld;
	
	public long getOutput() {
		return output;
	}

	public void setOutput(long output) {
		this.output = output;
	}

	public long getBadRecords() {
		return badRecords;
	}

	public void setBadRecords(long badRecords) {
		this.badRecords = badRecords;
	}

	public long getTotalRecordsNew() {
		return totalRecordsNew;
	}

	public void setTotalRecordsNew(long totalRecordsNew) {
		this.totalRecordsNew = totalRecordsNew;
	}

	public long getTotalRecordsOld() {
		return totalRecordsOld;
	}

	public void setTotalRecordsOld(long totalRecordsOld) {
		this.totalRecordsOld = totalRecordsOld;
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
			} else if ("-oldPath".equals(args[i])) {
				oldPath = args[++i];
			} else if ("-newPath".equals(args[i])) {
				newPath = args[++i];
			} else if ("-mergeBy".equals(args[i])) {
				mergeBy = args[++i];
			} else if ("-inputKeyClassName".equals(args[i])) {
				inputKeyClassName = args[++i];
			} else if ("-inputValueClassName".equals(args[i])) {
				inputValueClassName = args[++i];
			} else if ("-outputPath".equals(args[i])) {
				outputPath = args[++i];
			} else if ("-delimiter".equals(args[i])) {
				delimiter = args[++i];
			} else if ("-column".equals(args[i])) {
				column = Integer.parseInt(args[++i]);
			}else if ("-outputFormat".equals(args[i])) {
				outputFormat = args[++i];
			}
		}
	}

	public void checkMandatoryConfs() throws HIHOException {
		if (inputFormat == null) {
			throw new HIHOException(
					"The provided input format is empty, please specify inputFormat");
		}
		if (mergeBy == null) {
			throw new HIHOException(
					"The provided value of dedupBy is empty, please specify either key or value");
		}
		if ((!mergeBy.equals("key")) && (!mergeBy.equals("value"))) {
			throw new HIHOException(
					"The provided value of mergeBy is Incorrect, please specify either key or value");
		}
		if (inputKeyClassName == null) {
			throw new HIHOException(
					"The provided input key class name is empty, please specify inputKeyClassName");
		}
		if (inputValueClassName == null) {
			throw new HIHOException(
					"The provided input value class name is empty, please specify inputValueClassName");
		}
		if (oldPath == null) {
			throw new HIHOException(
					"The provided old path is empty, please specify oldPath");
		}
		if (newPath == null) {
			throw new HIHOException(
					"The provided new path is empty, please specify newPath");
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
		populateConfiguration(args);
		try {
			checkMandatoryConfs();
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new Exception(e1);
		}

		Class inputFormatClass = Class.forName(inputFormat);
		Class outputFormatClass = Class.forName(outputFormat);
		Class inputKeyClass = Class.forName(inputKeyClassName);
		Class inputValueClass = Class.forName(inputValueClassName);

		Configuration conf = getConf();
		conf.set(HIHOConf.MERGE_OLD_PATH, oldPath);
		conf.set(HIHOConf.MERGE_NEW_PATH, newPath);

		Job job = new Job(conf);
		job.setJobName("Merge job");
		job.setJarByClass(MergeJob.class);

		if (mergeBy.equals("key")) {
			job.setMapperClass(MergeKeyMapper.class);
			job.setReducerClass(MergeKeyReducer.class);

		} else if (mergeBy.equals("value")) {
			job.setMapperClass(MergeValueMapper.class);
			job.setReducerClass(MergeValueReducer.class);
		}

		job.setInputFormatClass(inputFormatClass);
		DelimitedTextInputFormat.setProperties(job, delimiter, column);
		job.setMapOutputKeyClass(HihoTuple.class);
		job.setMapOutputValueClass(HihoValue.class);

		job.setOutputKeyClass(inputKeyClass);
		job.setOutputValueClass(inputValueClass);
		FileInputFormat.setInputPaths(job, oldPath + "," + newPath);
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
				totalRecordsOld = counters.findCounter(
						MergeRecordCounter.TOTAL_RECORDS_OLD).getValue();
				totalRecordsNew = counters.findCounter(
						MergeRecordCounter.TOTAL_RECORDS_NEW).getValue();
				badRecords = counters.findCounter(
						MergeRecordCounter.BAD_RECORD).getValue();
				output = counters.findCounter(MergeRecordCounter.OUTPUT)
						.getValue();
				logger.info("Total old records read are: " + totalRecordsOld);
				logger.info("Total new records read are: " + totalRecordsNew);
				logger.info("Bad Records are: " + badRecords);
				logger.info("Output records are: " + output);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		MergeJob job = new MergeJob();
		int res = ToolRunner.run(new Configuration(), job, args);
		System.exit(res);

	}
}
