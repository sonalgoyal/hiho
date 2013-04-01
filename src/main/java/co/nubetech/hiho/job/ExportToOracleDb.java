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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.hiho.mapreduce.lib.db.apache.DBConfiguration;
import co.nubetech.hiho.mapreduce.lib.db.apache.MRJobConfig;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.OracleLoadMapper;
import co.nubetech.hiho.mapreduce.lib.input.FileStreamInputFormat;

public class ExportToOracleDb extends Configured implements Tool {
	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.job.ExportToOracleDb.class);

	private String inputPath = null;

	public void populateConfiguration(String[] args, Configuration conf) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			} else if ("-oracleFtpAddress".equals(args[i])) {
				conf.set(HIHOConf.ORACLE_FTP_ADDRESS, args[++i]);
			} else if ("-oracleFtpPortNumber".equals(args[i])) {
				conf.set(HIHOConf.ORACLE_FTP_PORT, args[++i]);
			} else if ("-oracleFtpUserName".equals(args[i])) {
				conf.set(HIHOConf.ORACLE_FTP_USER, args[++i]);
			} else if ("-oracleFtpPassword".equals(args[i])) {
				conf.set(HIHOConf.ORACLE_FTP_PASSWORD, args[++i]);
			} else if ("-oracleExternalTableDirectory".equals(args[i])) {
				conf.set(HIHOConf.ORACLE_EXTERNAL_TABLE_DIR, args[++i]);
			} else if ("-driver".equals(args[i])) {
				conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, args[++i]);
			} else if ("-url".equals(args[i])) {
				conf.set(DBConfiguration.URL_PROPERTY, args[++i]);
			} else if ("-userName".equals(args[i])) {
				conf.set(DBConfiguration.USERNAME_PROPERTY, args[++i]);
			} else if ("-password".equals(args[i])) {
				conf.set(DBConfiguration.PASSWORD_PROPERTY, args[++i]);
			} else if ("-externalTable".equals(args[i])) {
				conf.set(HIHOConf.EXTERNAL_TABLE_DML, args[++i]);
			}
		}
	}

	public void checkMandatoryConfs(Configuration conf) throws HIHOException {
		if (inputPath == null) {
			throw new HIHOException(
					"The provided inputPath is empty, please specify inputPath");
		}
		if (conf.get(HIHOConf.ORACLE_FTP_USER) == null) {
			throw new HIHOException(
					"The Oracle FTP UserName is not specified, please specify Oracle FTP UserName");
		}
		if (conf.get(HIHOConf.ORACLE_FTP_ADDRESS) == null) {
			throw new HIHOException(
					"The Oracle FTP Address is not specified, please specify Oracle FTP Address");
		}
		if (conf.get(HIHOConf.ORACLE_FTP_PORT) == null) {
			throw new HIHOException(
					"The Oracle FTP Port Number is not defined, please specify Oracle FTP Port Number");
		}
		if (conf.get(HIHOConf.ORACLE_FTP_PASSWORD) == null) {
			throw new HIHOException(
					"The Oracle FTP Password is not defined, please specify Oracle FTP Password");
		}
		if (conf.get(HIHOConf.ORACLE_EXTERNAL_TABLE_DIR) == null) {
			throw new HIHOException(
					"The Oracle External Table Directory is not specified, please specify Oracle External Table Directory");
		}
		if (conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY) == null) {
			throw new HIHOException(
					"The JDBC Driver is not specified, please specify JDBC Driver");
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
					"The JDBC PASSWORD is not specified, please specify JDBC PASSWORD");
		} 
		if (conf.get(HIHOConf.EXTERNAL_TABLE_DML) == null) {
			throw new HIHOException(
					"The query to create external table is not specified, please specify the create query for external table");
		}
		
	}

	@Override
	public int run(String[] args) throws IOException {
		Configuration conf = getConf();
		for (Entry<String, String> entry : conf) {
			logger.debug("key, value " + entry.getKey() + "="
					+ entry.getValue());
		}

		for (int i = 0; i < args.length; i++) {
			logger.debug("Remaining arguments are" + " " + args[i]);
		}
		populateConfiguration(args, conf);

		try {
			checkMandatoryConfs(conf);
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new IOException(e1);
		}

		Job job = new Job(conf);
		job.setJobName("OracleLoading");
		job.setMapperClass(OracleLoadMapper.class);
		job.setJarByClass(ExportToOracleDb.class);
		job.getConfiguration().setInt(MRJobConfig.NUM_MAPS,
				conf.getInt(HIHOConf.NUMBER_MAPPERS, 1));

		try {
			// we first create the external table definition
			String query = conf.get(HIHOConf.EXTERNAL_TABLE_DML);
			// create table if user has specified
			if (query != null) {
				this.runQuery(query, conf);
			}
		} catch (HIHOException e1) {

			e1.printStackTrace();
		}

		// verify required properties are loaded

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
		// run alter table query and add locations
		try {
			this.runQuery(getAlterTableDML(new Path(inputPath), conf), conf);
		} catch (HIHOException e1) {

			e1.printStackTrace();
		}
		return ret;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExportToOracleDb(),
				args);
		System.exit(res);
	}

	public static String getAlterTableDML(Path inputPath, Configuration conf)
			throws IOException, HIHOException {
		// after running the job, we need to alter the external table to take
		// care of the files we have added
		FileStatus[] contents = inputPath.getFileSystem(conf).listStatus(
				inputPath);
		if (contents != null) {
			StringBuilder dml = new StringBuilder();
			dml.append(" ALTER TABLE ");
			dml.append(getTableName(conf.get(HIHOConf.EXTERNAL_TABLE_DML)));
			dml.append(" LOCATION (");
			int i = 0;
			for (FileStatus content : contents) {
				String fileName = content.getPath().getName();
				dml.append("\'");
				dml.append(fileName);
				dml.append("\'");
				// add comma uptil one and last
				if (i < contents.length - 1) {
					dml.append(",");
				}
				i++;
			}
			// execute dml to alter location

			dml.append(")");
			return dml.toString();
		}
		return null;
	}

	public static String getTableName(String query) throws HIHOException {
		String tableName = null;
		if (query == null)
			throw new HIHOException("Cannot read query");

		try {
			StringTokenizer tokenizer = new StringTokenizer(query, "() ");
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.equalsIgnoreCase("table")) {
					tableName = tokenizer.nextToken();
				}
			}
		} catch (Exception e) {
			throw new HIHOException(
					"Unable to get table name from the external table query");
		}
		if (tableName == null) {
			throw new HIHOException(
					"Unable to get table name from the external table query");
		}
		return tableName;
	}

	public static String getExternalDir(String query) throws HIHOException {
		String tableName = null;
		if (query == null)
			throw new HIHOException("Cannot read query");

		try {
			StringTokenizer tokenizer = new StringTokenizer(query, " ");
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.equalsIgnoreCase("directory")) {
					tableName = tokenizer.nextToken();
				}
			}
		} catch (Exception e) {
			throw new HIHOException(
					"Unable to get directory name from the external table query");
		}
		if (tableName == null) {
			throw new HIHOException(
					"Unable to get directory name from the external table query");
		}
		return tableName;
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
			e.printStackTrace();
			throw new HIHOException("Sql syntax error in query " + query + e);
		}

	}

}