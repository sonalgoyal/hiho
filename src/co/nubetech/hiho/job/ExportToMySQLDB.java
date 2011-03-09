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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.MySQLLoadDataMapper;
import co.nubetech.hiho.mapreduce.lib.input.FileStreamInputFormat;

public class ExportToMySQLDB extends Configured implements Tool {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.job.ExportToMySQLDB.class);

	private String inputPath = null;

	public void populateConfiguration(String[] args, Configuration conf) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			} else if ("-url".equals(args[i])) {
				conf.set(DBConfiguration.URL_PROPERTY, args[++i]);
			} else if ("-userName".equals(args[i])) {
				conf.set(DBConfiguration.USERNAME_PROPERTY, args[++i]);
			} else if ("-password".equals(args[i])) {
				conf.set(DBConfiguration.PASSWORD_PROPERTY, args[++i]);
			} else if ("-querySuffix".equals(args[i])) {
				conf.set(HIHOConf.LOAD_QUERY_SUFFIX, args[++i]);
			}
		}
	}

	public void checkMandatoryConfs(Configuration conf) throws HIHOException {
		if (inputPath == null) {
			throw new HIHOException(
					"The provided inputPath is empty, please specify inputPath");
		}
		if (conf.get(DBConfiguration.URL_PROPERTY) == null) {
			throw new HIHOException(
					"The JDBC URL is not specified, please specify JDBC URL");
		}
		if (conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
			throw new HIHOException(
					"The JDBC USERNAME is not specified, please specify JDBC USERNAME");
		}
		if (conf.get(DBConfiguration.PASSWORD_PROPERTY) == null) {
			throw new HIHOException(
					"The JDBC PASSWORD is not defined, please specify JDBC PASSWORD");
		}
		if (conf.get(HIHOConf.LOAD_QUERY_SUFFIX) == null) {
			throw new HIHOException(
					"The Suffix for query is not defined, please specify suffix");
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
		FileStreamInputFormat.addInputPath(job, new Path(inputPath));
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		// job.setJarByClass(com.mysql.jdbc.Driver.class);
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
