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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import co.nubetech.apache.hadoop.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.hiho.mapreduce.MySQLLoadDataMapper;
import co.nubetech.hiho.mapreduce.lib.input.FileStreamInputFormat;

public class ExportToMySQLDB extends Configured implements Tool {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.job.ExportToMySQLDB.class);

	@Override
	public int run(String[] args) throws IOException {
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("MySQLBulkLoading");
		job.setMapperClass(MySQLLoadDataMapper.class);
		job.setJarByClass(MySQLLoadDataMapper.class);
		for (Entry<String, String> entry : conf) {
			logger.debug("key, value " + entry.getKey() + "="
					+ entry.getValue());
		}
		// verify required properties are loaded
		logger.debug(conf.get(DBConfiguration.URL_PROPERTY));
		logger.debug(conf.get(DBConfiguration.USERNAME_PROPERTY));
		logger.debug(conf.get(DBConfiguration.PASSWORD_PROPERTY));

		job.setNumReduceTasks(0);
		job.setInputFormatClass(FileStreamInputFormat.class);
		FileStreamInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		//job.setJarByClass(com.mysql.jdbc.Driver.class);
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
		int res = ToolRunner.run(new Configuration(), new ExportToMySQLDB(),
				args);
		System.exit(res);
	}

}
