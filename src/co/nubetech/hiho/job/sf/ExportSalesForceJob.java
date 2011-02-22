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
package co.nubetech.hiho.job.sf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.sf.SalesForceLoadMapper;

public class ExportSalesForceJob extends Configured implements Tool {

	private String inputPath = null;

	public void populateConfiguration(String[] args, Configuration conf) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			} else if ("-sfUserName".equals(args[i])) {
				conf.set(HIHOConf.SALESFORCE_USERNAME, args[++i]);
			} else if ("-sfPassword".equals(args[i])) {
				conf.set(HIHOConf.SALESFORCE_PASSWORD, args[++i]);
			} else if ("-sfObjectType".equals(args[i])) {
				conf.set(HIHOConf.SALESFORCE_SOBJECTYPE, args[++i]);
			} else if ("-sfHeaders".equals(args[i])) {
				conf.set(HIHOConf.SALESFORCE_HEADERS, args[++i]);
			}
		}
	}

	public void checkMandatoryConfs(Configuration conf) throws HIHOException {
		if (inputPath == null) {
			throw new HIHOException(
					"The provided inputPath is empty, please specify inputPath");
		}
		if (conf.get(HIHOConf.SALESFORCE_USERNAME) == null) {
			throw new HIHOException(
					"The SalesForce UserName is not specified, please specify SalesForce UserName");
		}
		if (conf.get(HIHOConf.SALESFORCE_PASSWORD) == null) {
			throw new HIHOException(
					"The SalesForce Password is not specified, please specify SalesForce Password");
		}
		if (conf.get(HIHOConf.SALESFORCE_SOBJECTYPE) == null) {
			throw new HIHOException(
					"The SalesForce SOBJECTYPE is not specified, please specify SalesForce SOBJECTYPE");
		}
		if (conf.get(HIHOConf.SALESFORCE_HEADERS) == null) {
			throw new HIHOException(
					"The SalesForce Headers is not specified, please specify SalesForce Headers");
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		populateConfiguration(arg0, conf);
		try {
			checkMandatoryConfs(conf);
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new Exception(e1);
		}

		Job job = new Job(conf);
		job.setJobName("SaleForceLoading");
		job.setMapperClass(SalesForceLoadMapper.class);
		job.setJarByClass(SalesForceLoadMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputPath));
		// NLineInputFormat.setNumLinesPerSplit(job, 10);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(NullOutputFormat.class);

		int ret = 0;

		try {
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new ExportSalesForceJob(), args);
		System.exit(res);
	}

}
