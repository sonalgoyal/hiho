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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOException;

public class NGramJob extends Configured implements Tool {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.similarity.ngram.NGramJob.class);
	
	private String inputPath = null;
	
	public void populateConfiguration(String[] args) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			} 
		}
	}
	
	public void checkMandatoryConfs() throws HIHOException {
		if (inputPath == null) {
			throw new HIHOException(
					"The provided input path is empty, please specify inputPath");
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
		job.setJobName("NGram job");
		job.setJarByClass(NGramJob.class);

		Class inputFormatClass = Class
				.forName("org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat");
		Class outputFormatClass = Class
				.forName("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
		// org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
		// org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
		Class inputKeyClass = Class.forName("org.apache.hadoop.io.Text");
		Class inputValueClass = Class.forName("org.apache.hadoop.io.Text");
		Class outputKeyClass = Class
				.forName("co.nubetech.hiho.similarity.ngram.ValuePair");
		Class outputValueClass = Class
				.forName("org.apache.hadoop.io.IntWritable");

		job.setMapperClass(NGramMapper.class);
		job.setReducerClass(NGramReducer.class);

		job.setInputFormatClass(inputFormatClass);
		job.setMapOutputKeyClass(inputKeyClass);
		job.setMapOutputValueClass(inputValueClass);

		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		job.setOutputFormatClass(outputFormatClass);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path("outputOfNGramJob"));

		int ret = 0;
		try {
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static void main(String[] args) throws Exception {
		NGramJob job = new NGramJob();
		int res = ToolRunner.run(new Configuration(), job, args);
		System.exit(res);
	}

}
