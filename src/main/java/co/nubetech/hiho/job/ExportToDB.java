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
package co.nubetech.hiho.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import co.nubetech.hiho.mapreduce.lib.db.apache.DBConfiguration;
import co.nubetech.hiho.mapreduce.lib.db.apache.MRJobConfig;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.GenericDBLoadDataMapper;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBOutputFormat;

public class ExportToDB extends Configured implements Tool {

	private String inputPath = null;
	private String tableName = null;
	private String columnNames = null;

	public void populateConfiguration(String[] args, Configuration conf) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-jdbcDriver".equals(args[i])) {
				conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, args[++i]);
			} else if ("-jdbcUrl".equals(args[i])) {
				conf.set(DBConfiguration.URL_PROPERTY, args[++i]);
			} else if ("-jdbcUsername".equals(args[i])) {
				conf.set(DBConfiguration.USERNAME_PROPERTY, args[++i]);
			} else if ("-jdbcPassword".equals(args[i])) {
				conf.set(DBConfiguration.PASSWORD_PROPERTY, args[++i]);
			} else if ("-delimiter".equals(args[i])) {
				conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, args[++i]);
			} else if ("-numberOfMappers".equals(args[i])) {
				conf.set(HIHOConf.NUMBER_MAPPERS, args[++i]);
			} else if ("-tableName".equals(args[i])) {
				tableName = args[++i];
			} else if ("-columnNames".equals(args[i])) {
				columnNames = args[++i];
			} else if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			}
		}
	}

	public void checkMandatoryConfs(Configuration conf) throws HIHOException {
		if (conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY) == null) {
			throw new HIHOException(
					"JDBC driver configuration is not specified,please specify JDBC driver class.");
		}
		if (conf.get(DBConfiguration.URL_PROPERTY) == null) {
			throw new HIHOException(
					"JDBC url path configuration is empty,please specify JDBC url path.");
		}
		if (!conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY).contains("hsqldb")) {
			if (conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
				throw new HIHOException(
						"JDBC user name configuration is empty,please specify JDBC user name.");
			}
			if (conf.get(DBConfiguration.PASSWORD_PROPERTY) == null) {
				throw new HIHOException(
						"JDBC password configuration is empty,please specify JDBC password.");
			}
		}
		if (conf.get(HIHOConf.INPUT_OUTPUT_DELIMITER) == null) {
			throw new HIHOException(
					"The provided delimiter is empty, please specify delimiter.");
		}
		if (conf.get(HIHOConf.NUMBER_MAPPERS) == null) {
			throw new HIHOException(
					"The provided number of mappers is empty, please specify number of mappers.");
		}
		if (inputPath == null) {
			throw new HIHOException(
					"The provided input path is empty, please specify inputPath.");
		}
		if (tableName == null) {
			throw new HIHOException(
					"The provided table name is empty, please specify tableName.");
		}
		if (columnNames == null) {
			throw new HIHOException(
					"The provided column name is empty, please specify columnName.");
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		populateConfiguration(args, conf);
		try {
			checkMandatoryConfs(conf);
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new Exception(e1);
		}		
		Job job = new Job(conf);
		job.getConfiguration().setInt(MRJobConfig.NUM_MAPS,
				conf.getInt(HIHOConf.NUMBER_MAPPERS, 1));
		job.setJobName("HihoDBExport");

		job.setMapperClass(GenericDBLoadDataMapper.class);
		job.setJarByClass(ExportToDB.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputPath));
		GenericDBOutputFormat.setOutput(job, tableName, columnNames);

		int ret = 0;
		try {
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExportToDB(), args);
		System.exit(res);
	}

}
