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

public class ScoreJob extends Configured implements Tool {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.similarity.ngram.ScoreJob.class);

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("Score job");
		job.setJarByClass(ScoreJob.class);

		Class inputFormatClass = Class
				.forName("org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
		Class outputFormatClass = Class
				.forName("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
		// org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
		// org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
		Class inputKeyClass = Class
				.forName("co.nubetech.hiho.similarity.ngram.ValuePair");
		Class inputValueClass = Class
				.forName("org.apache.hadoop.io.IntWritable");
		Class outputKeyClass = Class
				.forName("co.nubetech.hiho.similarity.ngram.ValuePair");
		Class outputValueClass = Class
				.forName("org.apache.hadoop.io.LongWritable");

		job.setMapperClass(ScoreMapper.class);
		job.setReducerClass(ScoreReducer.class);

		job.setInputFormatClass(inputFormatClass);
		job.setMapOutputKeyClass(inputKeyClass);
		job.setMapOutputValueClass(inputValueClass);

		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		job.setOutputFormatClass(outputFormatClass);

		FileInputFormat.setInputPaths(job, "outputOfNGramJob");
		FileOutputFormat.setOutputPath(job, new Path("outputOfScoreJob"));

		int ret = 0;
		try {
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static void main(String[] args) throws Exception {
		ScoreJob job = new ScoreJob();
		int res = ToolRunner.run(new Configuration(), job, args);
		System.exit(res);
	}

}
