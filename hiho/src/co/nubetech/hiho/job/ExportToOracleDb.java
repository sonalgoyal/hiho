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

import java.sql.*;
import java.util.StringTokenizer;
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

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.MySQLLoadDataMapper;
import co.nubetech.hiho.mapreduce.OracleLoadMapper;
import co.nubetech.hiho.mapreduce.lib.input.FileStreamInputFormat;

public class ExportToOracleDb extends Configured implements Tool {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.job.ExportToOracleDb.class);

	@Override
	public int run(String[] args) throws IOException {
		Configuration conf = getConf();

		for (Entry<String, String> entry : conf) {
			logger.debug("key, value " + entry.getKey() + "="
					+ entry.getValue());
		}

		// we first create the external table definition
		String query = conf.get(HIHOConf.EXTERNAL_TABLE_DML);

		String filename = "";
		try {
			filename = this.getFilename(query);
		} catch (HIHOException e) {

			e.printStackTrace();
		}
		conf.set(HIHOConf.EXTERNAL_TABLE_FILENAME, filename);
		logger.debug("Setting key and value "
				+ HIHOConf.EXTERNAL_TABLE_FILENAME + " : " + filename);

		Job job = new Job(conf);
		job.setJobName("OracleLoading");
		job.setMapperClass(OracleLoadMapper.class);
		job.setJarByClass(ExportToOracleDb.class);

		try {
			this.runQuery(query, conf);
		} catch (HIHOException e1) {

			e1.printStackTrace();
		}
		
		// verify required properties are loaded

		job.setNumReduceTasks(0);
		job.setInputFormatClass(FileStreamInputFormat.class);
		FileStreamInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		// job.setJarByClass(com.mysql.jdbc.Driver.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		int ret = 0;
		try {
			logger.debug("Before runnign the job, key and value "
					+ HIHOConf.EXTERNAL_TABLE_FILENAME + " : " + filename);
			// job.getConfiguration().set(HIHOConf.EXTERNAL_TABLE_FILENAME,
			// filename);
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExportToOracleDb(),
				args);
		System.exit(res);
	}

	public String getFilename(String query) throws HIHOException {
		String[] temp2 = new String[10];
		if (query == null)
			throw new HIHOException("Cannot read query");
		int i = query.indexOf("location");
		if (i == -1)
			throw new HIHOException(
					"Wrong configuration for external table cannot find filename for table");

		int l = query.indexOf("(", i);
		if (l == -1)
			throw new HIHOException(
					"Wrong configuration for external table cannot find filename for table");

		int r = query.indexOf(")", l + 1);
		if (r == -1)
			throw new HIHOException(
					"Wrong configuration for external table cannot find filename for table");

		String temp = query.substring(l, r);

		l = temp.indexOf("'");
		if (l == -1)
			throw new HIHOException(
					"Wrong configuration for external table cannot find filename for table");

		r = temp.indexOf("'", l + 1);
		if (r == -1)
			throw new HIHOException(
					"Wrong configuration for external table cannot find filename for table");

		temp = temp.substring(l + 1, r);

		return temp.trim();

	}

	/**
	 * This function creates the external table. the following queries have been
	 * tested: create table age( i Number, n Varchar(20), a Number )
	 * organization external ( type oracle_loader default directory ext_dir
	 * access parameters ( records delimited by newline fields terminated by ','
	 * missing field values are null ) location('file.txt') ) reject limit
	 * unlimited
	 * 
	 * 
	 * The following queries are bound to fail create table ( i Number, n
	 * Varchar(20), a Number ) organization external ( type oracle_loader
	 * default directory ext_dir access parameters ( records delimited by
	 * newline fields terminated by ',' missing field values are null )
	 * location( 'file.txt' ) ) reject limit unlimited
	 * 
	 * @param query
	 * @param conf
	 * @throws HIHOException
	 */
	public void runQuery(String query, Configuration conf) throws HIHOException {
		try {
			String driver = conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY);
			String url = conf.get(DBConfiguration.URL_PROPERTY);
			String usr = conf.get(DBConfiguration.USERNAME_PROPERTY);
			String pswd = conf.get(DBConfiguration.PASSWORD_PROPERTY);

			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, usr, pswd);

			Statement stmt = conn.createStatement();

			stmt.executeQuery(query);

			stmt.close();
			conn.close();

		} catch (Exception e) {
			// e.printStackTrace();
			throw new HIHOException(
					"Sql syntax error in query cannot create external table\n"
							+ e);
		}

	}

}