/**
 * Copyright 2010 Nube Technologies
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

package co.nubetech.hiho.job;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.lib.output.FTPTextOutputFormat;

public class ExportToFTPServer extends Configured implements Tool {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.job.ExportToFTPServer.class);

	private String inputPath = null;
	private String outputPath = null;

	public void populateConfiguration(String[] args, Configuration conf) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			} else if ("-outputPath".equals(args[i])) {
				outputPath = args[++i];
			} else if ("-ftpUserName".equals(args[i])) {
				conf.set(HIHOConf.FTP_USER, args[++i]);
			} else if ("-ftpAddress".equals(args[i])) {
				conf.set(HIHOConf.FTP_ADDRESS, args[++i]);
			} else if ("-ftpPortNumper".equals(args[i])) {
				conf.set(HIHOConf.FTP_PORT, args[++i]);
			} else if ("-ftpPassword".equals(args[i])) {
				conf.set(HIHOConf.FTP_PASSWORD, args[++i]);
			}
		}
	}

	public void checkMandatoryConfs(Configuration conf) throws HIHOException {
		if (inputPath == null) {
			throw new HIHOException(
					"The provided inputPath is empty, please specify inputPath");
		}
		if (outputPath == null) {
			throw new HIHOException(
					"The outputPath is not defined, please specify outputPath");
		}
		if (conf.get(HIHOConf.FTP_USER) == null) {
			throw new HIHOException(
					"The FTP UserName is not defined, please specify FTP UserName");
		}
		if (conf.get(HIHOConf.FTP_ADDRESS) == null) {
			throw new HIHOException(
					"The FTP Address is not defined, please specify FTP Address");
		}
		if (conf.get(HIHOConf.FTP_PORT) == null) {
			throw new HIHOException(
					"The FTP Port Number is not defined, please specify FTP Port Number");
		}
		if (conf.get(HIHOConf.FTP_PASSWORD) == null) {
			throw new HIHOException(
					"The FTP Password is not defined, please specify FTP Password");
		}
	}

	@Override
	public int run(String[] args) throws IOException {
		Configuration conf = getConf();
		populateConfiguration(args, conf);
		try {
			checkMandatoryConfs(conf);
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new IOException(e1);
		}

		for (Entry<String, String> entry : conf) {
			logger.debug("key, value " + entry.getKey() + "="
					+ entry.getValue());
		}
		Job job = new Job(conf);
		job.setMapperClass(TokenCounterMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputPath));
		job.setReducerClass(IntSumReducer.class);
		job.setOutputFormatClass(FTPTextOutputFormat.class);
		FTPTextOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setJarByClass(ExportToFTPServer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);

		int ret = 0;

		try {
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExportToFTPServer(),
				args);
		System.exit(res);
	}

}
