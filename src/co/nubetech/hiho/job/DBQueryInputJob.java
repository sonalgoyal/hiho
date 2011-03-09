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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.apache.hadoop.DataDrivenDBInputFormat;
import co.nubetech.apache.hadoop.MRJobConfig;
import co.nubetech.hiho.avro.DBMapper;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.OutputStrategyEnum;
import co.nubetech.hiho.hive.HiveUtility;
import co.nubetech.hiho.mapred.avro.GenericRecordAvroOutputFormat;
import co.nubetech.hiho.mapreduce.DBInputAvroMapper;
import co.nubetech.hiho.mapreduce.DBInputDelimMapper;
import co.nubetech.hiho.mapreduce.lib.db.DBQueryInputFormat;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;
import co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat;
import co.nubetech.hiho.pig.PigUtility;

public class DBQueryInputJob extends Configured implements Tool {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.job.DBQueryInputJob.class);

	private ArrayList params;

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
			} else if ("-inputQuery".equals(args[i])) {
				conf.set(DBConfiguration.INPUT_QUERY, args[++i]);
			} else if ("-inputBoundingQuery".equals(args[i])) {
				conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, args[++i]);
			} else if ("-outputPath".equals(args[i])) {
				conf.set(HIHOConf.INPUT_OUTPUT_PATH, args[++i]);
			} else if ("-outputStrategy".equals(args[i])) {
				conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, args[++i]);
			} else if ("-delimiter".equals(args[i])) {
				conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, args[++i]);
			} else if ("-numberOfMappers".equals(args[i])) {
				conf.set(HIHOConf.NUMBER_MAPPERS, args[++i]);
			} else if ("-inputTableName".equals(args[i])) {
				conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, args[++i]);
			} else if ("-inputFieldNames".equals(args[i])) {
				conf.set(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, args[++i]);
			} else if ("-inputOrderBy".equals(args[i])) {
				conf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, args[++i]);
			} else if ("-inputLoadTo".equals(args[i])) {
				conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, args[++i]);
			} else if ("-inputLoadToPath".equals(args[i])) {
				conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, args[++i]);
			} else if ("-hiveDriver".equals(args[i])) {
				conf.set(HIHOConf.HIVE_DRIVER, args[++i]);
			} else if ("-hiveUrl".equals(args[i])) {
				conf.set(HIHOConf.HIVE_URL, args[++i]);
			} else if ("-hiveUsername".equals(args[i])) {
				conf.set(HIHOConf.HIVE_USR_NAME, args[++i]);
			} else if ("-hivePassword".equals(args[i])) {
				conf.set(HIHOConf.HIVE_PASSWORD, args[++i]);
			} else if ("-hivePartitionBy".equals(args[i])) {
				conf.set(HIHOConf.HIVE_PARTITION_BY, args[++i]);
			} else if ("-hiveIfNotExists".equals(args[i])) {
				conf.set(HIHOConf.HIVE_TABLE_OVERWRITE, args[++i]);
			} else if ("-hiveTableName".equals(args[i])) {
				conf.set(HIHOConf.HIVE_TABLE_NAME, args[++i]);
			} else if ("-hiveSortedBy".equals(args[i])) {
				conf.set(HIHOConf.HIVE_SORTED_BY, args[++i]);
			} else if ("-hiveClusteredBy".equals(args[i])) {
				conf.set(HIHOConf.HIVE_CLUSTERED_BY, args[++i]);
			}
		}
	}

	public void checkMandatoryConfs(Configuration conf) throws HIHOException {
		if (conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY) == null) {
			throw new HIHOException(
					"JDBC driver configuration is not specified,please specify JDBC driver class");
		}
		if (conf.get(DBConfiguration.URL_PROPERTY) == null) {
			throw new HIHOException(
					"JDBC url path configuration is empty,please specify JDBC url path");
		}
		if (!conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY).contains("hsqldb")) {
			if (conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
				throw new HIHOException(
						"JDBC user name configuration is empty,please specify JDBC user name");
			}
			if (conf.get(DBConfiguration.PASSWORD_PROPERTY) == null) {
				throw new HIHOException(
						"JDBC password configuration is empty,please specify JDBC password");
			}
		}
		if (conf.get(HIHOConf.INPUT_OUTPUT_PATH) == null) {
			throw new HIHOException(
					"Output path is not specified,please specify output path");
		}
		if (conf.get(HIHOConf.INPUT_OUTPUT_STRATEGY) != null
				&& conf.get(HIHOConf.INPUT_OUTPUT_STRATEGY).equals("DELIMITED")) {
			if (conf.get(HIHOConf.INPUT_OUTPUT_DELIMITER) == null) {
				throw new HIHOException(
						"Delimiter is not specified,please specify delimiter");
			}

		}
		if (conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY) == null
				&& conf.get(DBConfiguration.INPUT_QUERY) == null) {
			throw new HIHOException(
					"Input table name and input query both configurations are empty, please specify anyone of them");
		}
		if (conf.get(DBConfiguration.INPUT_QUERY) != null
				&& conf.get(DBConfiguration.INPUT_BOUNDING_QUERY) == null) {
			throw new HIHOException(
					"Please specify input bounding query as it is mandatory to be defined with input query ");
		}
		if (conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY) != null
				&& conf.get(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY) == null) {
			conf.set(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, "*");
		}

		if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO) != null
				&& conf.get(HIHOConf.INPUT_OUTPUT_LOADTO_PATH) == null) {
			throw new HIHOException(
					"Load to path configuration is empty, please specify path to load script in loadTOPath configuration");
		}
		if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO) != null
				&& conf.get(HIHOConf.INPUT_OUTPUT_LOADTO).equals("hive")) {
			if (conf.get(HIHOConf.HIVE_URL) == null) {
				throw new HIHOException(
						"The Hive url is not defined, please specify hive url");
			}
			if (conf.get(HIHOConf.HIVE_DRIVER) == null) {
				throw new HIHOException(
						"The Hive driver is not defined, please specify hive driver");
			}
			if (checkForMultiplePartition(conf.get(HIHOConf.HIVE_PARTITION_BY))
					&& conf.get(HIHOConf.HIVE_TABLE_NAME) == null) {
				throw new HIHOException("please specify hive table name");
			}

		}

	}

	@Override
	public int run(String[] args) throws IOException {

		Configuration conf = getConf();
		populateConfiguration(args, conf);

		boolean isMultiplePartition = false;

		if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO) != null) {
			if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO).equals("hive")) {
				conf.set("hadoop.job.history.user.location", "none");
				if (conf.get(HIHOConf.HIVE_PARTITION_BY) != null) {
					try {
						isMultiplePartition = checkForMultiplePartition(conf
								.get(HIHOConf.HIVE_PARTITION_BY));
					} catch (HIHOException e) {
						e.printStackTrace();
					}

				}
			}

			if (isMultiplePartition
					&& conf.get(HIHOConf.INPUT_OUTPUT_LOADTO).equals("hive")) {
				populateHiveConfigurationForMultiplePartition(conf);
			} else {
				runJobs(conf, 0);
			}

		} else {
			runJobs(conf, 0);
		}
		return 0;
	}

	private boolean checkForMultiplePartition(String partitionBy)
			throws HIHOException {
		// ArrayList<PartitionBy> partitionByValues= new
		// ArrayList<PartitionBy>();
		boolean isMultiplePartition = false;
		StringTokenizer partitionToken = new StringTokenizer(partitionBy, ";");
		StringTokenizer partitionData = new StringTokenizer(
				partitionToken.nextToken(), ":");
		try {
			partitionData.nextToken();
			partitionData.nextToken();
		} catch (NoSuchElementException e) {
			throw new HIHOException(
					"Data not defined properly in partitionBy configuration");
		}
		if (partitionData.hasMoreTokens()) {
			int index = partitionData.nextToken().indexOf(",");
			if (index > -1) {
				isMultiplePartition = true;
				if (partitionToken.hasMoreTokens()) {
					throw new HIHOException(
							"Data not defined properly in partitionBy configuration");

				}

			}
		}
		return isMultiplePartition;
	}

	private void generatePigScript(Configuration conf, Job job)
			throws HIHOException {
		// /see if import to pig or hive
		if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO).equals("pig")) {
			try {
				String pigScript = PigUtility.getLoadScript(
						HIHOConf.INPUT_OUTPUT_PATH, getDBWritable(conf));
				// //jobId
				File file = new File(new File(
						conf.get(HIHOConf.INPUT_OUTPUT_LOADTO_PATH)),
						"pigScript" + ".txt");
				FileOutputStream fos = new FileOutputStream(file);
				BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
						fos));
				w.write(pigScript);
				w.close();
				fos.close();
			} catch (Exception h) {
				throw new HIHOException("Unable to generate Pig script", h);
			}
		}

	}

	private void generateHiveScript(Configuration conf, Job job, int jobCounter)
			throws HIHOException {
		// /see if import to pig or hive
		if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO).equals("hive")) {
			try {
				HiveUtility.createTable(conf, job, getDBWritable(conf),
						jobCounter);
			} catch (Exception h) {
				throw new HIHOException("Unable to generate Hive script", h);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		// setUp();
		DBQueryInputJob job = new DBQueryInputJob();
		// ArrayList params = new ArrayList();
		// params.add(false);
		// job.setParams(params);
		int res = ToolRunner.run(new Configuration(), job, args);
		System.exit(res);
	}

	public ArrayList getParams() {
		return params;
	}

	public void setParams(ArrayList params) {
		this.params = params;
	}

	/*
	 * this will move to the junit once everything is properly done
	 * 
	 * public static void setUp() { // set up the database String db =
	 * "cumulus"; String root = "root"; String pwd = "newpwd";
	 * 
	 * String user = "tester"; String password = "password";
	 * 
	 * Connection conn; String url = "jdbc:mysql://localhost:3306/"; String
	 * driverName = "com.mysql.jdbc.Driver";
	 * 
	 * try { Class.forName(driverName).newInstance(); conn =
	 * DriverManager.getConnection(url, root, pwd); try { Statement st =
	 * conn.createStatement();
	 * 
	 * String dbDrop = "drop database if exists " + db;
	 * st.executeUpdate(dbDrop); logger.debug("Dropped database");
	 * 
	 * String dbCreate = "create database " + db; st.executeUpdate(dbCreate);
	 * logger.debug("Created database");
	 * 
	 * // Register a new user named tester on the // database named cumulus with
	 * a password // password enabling several different // privileges.
	 * st.executeUpdate("GRANT SELECT,INSERT,UPDATE,DELETE," + "CREATE,DROP " +
	 * "ON " + db + ".* TO '" + user + "'@'localhost' " + "IDENTIFIED BY '" +
	 * password + "';"); logger.debug("Created user tester"); st.close();
	 * 
	 * // now connect to the relevent db and create the schema conn =
	 * DriverManager.getConnection(url + db, root, pwd); st =
	 * conn.createStatement();
	 * 
	 * String desTable =
	 * "Create table if not exists designations(id integer, designation varchar(30));\n"
	 * ; st = conn.createStatement(); st.executeUpdate(desTable);
	 * 
	 * logger.debug(desTable);
	 * 
	 * String desTableData =
	 * "insert into designations(id, designation) values("; desTableData +=
	 * "0, 'Manager');\n"; st.executeUpdate(desTableData); desTableData =
	 * "insert into designations(id, designation) values("; desTableData +=
	 * "1, 'Accountant');\n"; st.executeUpdate(desTableData); desTableData =
	 * "insert into designations(id, designation) values("; desTableData +=
	 * "2, 'Assistant');\n"; st.executeUpdate(desTableData); desTableData =
	 * "insert into designations(id, designation) values("; desTableData +=
	 * "3, 'Sr. Manager');\n"; logger.debug(desTableData);
	 * st.executeUpdate(desTableData);
	 * logger.debug("Data inserte4d into designations");
	 * 
	 * String table =
	 * "CREATE TABLE if not exists extractJobEmployee(id integer, name varchar(50), age integer"
	 * ; table += ", isMarried boolean, salary double, designationId integer);";
	 * st = conn.createStatement(); st.executeUpdate(table);
	 * 
	 * logger.debug("Schema creation process successfull!");
	 * logger.debug("Inserting table data");
	 * 
	 * for (int i = 0; i < 50; ++i) { int designation = i % 4; String tableData
	 * =
	 * "Insert into extractJobEmployee(id, name, age, isMarried, salary, designationId) values("
	 * ; tableData += i + ", 'Employee" + i; tableData += "', 25, false, 349.9,"
	 * + designation + ");\n"; logger.debug(tableData); st =
	 * conn.createStatement(); st.executeUpdate(tableData); } // conn.commit();
	 * logger.debug("Table data insertion process successfull!"); } catch
	 * (SQLException s) { s.printStackTrace(); } conn.close(); } catch
	 * (Exception e) { e.printStackTrace(); }
	 * 
	 * }
	 */

	public static GenericDBWritable getDBWritable(Configuration conf)
			throws HIHOException {
		try {
			String driverName = conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY);
			String url = conf.get(DBConfiguration.URL_PROPERTY);
			String user = conf.get(DBConfiguration.USERNAME_PROPERTY);
			String password = conf.get(DBConfiguration.PASSWORD_PROPERTY);
			Class.forName(driverName).newInstance();
			Connection conn = DriverManager.getConnection(url, user, password);
			DatabaseMetaData dbMeta = conn.getMetaData();
			String dbProductName = dbMeta.getDatabaseProductName()
					.toUpperCase();
			String query = getSelectQuery(conf, dbProductName);
			PreparedStatement st = conn.prepareStatement(query);
			logger.warn("\n Query for GenericDBWritable is " + query);
			GenericDBWritable writable = new GenericDBWritable(
					GenericDBWritable.populateColumnInfo(st.getMetaData()),
					null);
			return writable;

		} catch (Exception e) {
			e.printStackTrace();
			throw new HIHOException("Unable to get metadata for the query", e);
		}
	}

	/**
	 * Returns the query for selecting the records, subclasses can override this
	 * for custom behaviour. A lot is actually a copy from
	 * DataDrivenDBRecordReader
	 */
	@SuppressWarnings("unchecked")
	public static String getSelectQuery(Configuration conf, String dbProductName)
			throws HIHOException {

		StringBuilder query = new StringBuilder();
		DBConfiguration dbConf = new DBConfiguration(conf);
		String[] fieldNames = dbConf.getInputFieldNames();
		String tableName = dbConf.getInputTableName();
		String conditions = dbConf.getInputConditions();
		StringBuilder conditionClauses = new StringBuilder();

		if (dbConf.getInputQuery() == null) {
			// We need to generate the entire query.
			query.append("SELECT ");

			for (int i = 0; i < fieldNames.length; i++) {
				query.append(fieldNames[i]);
				if (i != fieldNames.length - 1) {
					query.append(", ");
				}
			}

			query.append(" FROM ").append(tableName);
			if (!dbProductName.startsWith("ORACLE")) {
				// Seems to be necessary for hsqldb? Oracle explicitly does
				// *not*
				// use this clause.
				query.append(" AS ").append(tableName);
			}

		} else {
			// User provided the query. We replace the special token with our
			// WHERE clause.
			String inputQuery = dbConf.getInputQuery();
			if (inputQuery.indexOf(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
				logger.error("Could not find the clause substitution token "
						+ DataDrivenDBInputFormat.SUBSTITUTE_TOKEN
						+ " in the query: [" + inputQuery
						+ "]. Parallel splits may not work correctly.");
			}
			// bad bad hack, but we dont have the split here..
			conditionClauses.append("( 1=1 )");

			query.append(inputQuery.replace(
					DataDrivenDBInputFormat.SUBSTITUTE_TOKEN,
					conditionClauses.toString()));
		}

		return query.toString();

	}

	public void populateHiveConfigurationForMultiplePartition(Configuration conf)
			throws IOException {

		String columnName = null, columnType = null;
		ArrayList columnsValues = new ArrayList();

		ArrayList query = new ArrayList();
		ArrayList table = new ArrayList();

		String partitionByData = conf.get(HIHOConf.HIVE_PARTITION_BY);
		boolean isInputQueryDelimited = false;
		String queries = null;
		if (conf.get(DBConfiguration.INPUT_QUERY) != null) {
			isInputQueryDelimited = true;
			queries = conf.get(DBConfiguration.INPUT_QUERY);
		}
		// /
		StringTokenizer partitionByTokens = new StringTokenizer(
				partitionByData, ":");
		columnName = partitionByTokens.nextToken();
		columnType = partitionByTokens.nextToken();
		StringTokenizer partitionByValues = new StringTokenizer(
				partitionByTokens.nextToken(), ",");
		int counter = 0;
		int tokenCounts = partitionByValues.countTokens();
		while (partitionByValues.hasMoreTokens()) {
			columnsValues.add(counter, partitionByValues.nextToken());
			counter++;
		}
		// /
		if (isInputQueryDelimited) {
			StringTokenizer queryTokens = new StringTokenizer(queries, ";");
			counter = 0;
			while (queryTokens.hasMoreTokens()) {
				query.add(counter, queryTokens.nextToken());
				counter++;
			}
		} else {
			StringTokenizer tableTokens = new StringTokenizer(
					conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY), ";");
			counter = 0;
			while (tableTokens.hasMoreTokens()) {
				table.add(counter, tableTokens.nextToken());
				counter++;
			}

		}
		for (int jobCounter = 0; jobCounter < tokenCounts; jobCounter++) {
			if (isInputQueryDelimited) {
				;
				conf.set(DBConfiguration.INPUT_QUERY, query.get(jobCounter)
						.toString());
			} else {
				conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY,
						table.get(jobCounter).toString());
			}
			conf.set(HIHOConf.INPUT_OUTPUT_PATH,
					conf.get(HIHOConf.INPUT_OUTPUT_PATH) + jobCounter);
			// conf.set(HIHOConf.HIVE_TABLE_OVERWRITE, "true");
			String partitionBy = columnName + ":" + columnType + ":"
					+ columnsValues.get(jobCounter).toString();
			conf.set(HIHOConf.HIVE_PARTITION_BY, partitionBy);
			runJobs(conf, jobCounter);

		}

	}

	public void runJobs(Configuration conf, int jobCounter) throws IOException {

		try {
			checkMandatoryConfs(conf);
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new IOException(e1);
		}

		Job job = new Job(conf);
		for (Entry<String, String> entry : conf) {
			logger.warn("key, value " + entry.getKey() + "=" + entry.getValue());
		}

		// logger.debug("Number of maps " +
		// conf.getInt("mapred.map.tasks", 1));
		// conf.setInt(JobContext.NUM_MAPS,
		// conf.getInt("mapreduce.job.maps", 1));
		// job.getConfiguration().setInt("mapred.map.tasks", 4);
		job.getConfiguration().setInt(MRJobConfig.NUM_MAPS,
				conf.getInt(HIHOConf.NUMBER_MAPPERS, 1));
		logger.warn("Number of maps " + conf.getInt(MRJobConfig.NUM_MAPS, 1));

		job.setJobName("Import job");
		job.setJarByClass(DBQueryInputJob.class);

		String strategy = conf.get(HIHOConf.INPUT_OUTPUT_STRATEGY);
		OutputStrategyEnum os = OutputStrategyEnum.value(strategy);
		if (os == null) {
			throw new IllegalArgumentException(
					"Wrong value of output strategy. Please correct");
		}
		if (os != OutputStrategyEnum.AVRO) {
			switch (os) {

			case DUMP: {
				// job.setMapperClass(DBImportMapper.class);
				break;
			}
				/*
				 * case AVRO: { job.setMapperClass(DBInputAvroMapper.class); //
				 * need avro in cp // job.setJarByClass(Schema.class); // need
				 * jackson which is needed by avro - ugly! //
				 * job.setJarByClass(ObjectMapper.class);
				 * job.setMapOutputKeyClass(NullWritable.class);
				 * job.setMapOutputValueClass(AvroValue.class);
				 * job.setOutputKeyClass(NullWritable.class);
				 * job.setOutputValueClass(AvroValue.class);
				 * job.setOutputFormatClass(AvroOutputFormat.class);
				 * 
				 * AvroOutputFormat.setOutputPath(job, new
				 * Path(getConf().get(HIHOConf.INPUT_OUTPUT_PATH))); break; }
				 */
			case DELIMITED: {
				job.setMapperClass(DBInputDelimMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setOutputFormatClass(NoKeyOnlyValueOutputFormat.class);

				NoKeyOnlyValueOutputFormat.setOutputPath(job, new Path(
						getConf().get(HIHOConf.INPUT_OUTPUT_PATH)));
			}
			case JSON: {
				// job.setMapperClass(DBImportJsonMapper.class);
				// job.setJarByClass(ObjectMapper.class);
				break;
			}
			default: {
				job.setMapperClass(DBInputDelimMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setOutputFormatClass(NoKeyOnlyValueOutputFormat.class);

				NoKeyOnlyValueOutputFormat.setOutputPath(job, new Path(
						getConf().get(HIHOConf.INPUT_OUTPUT_PATH)));
				break;
			}
			}

			String inputQuery = conf.get(DBConfiguration.INPUT_QUERY);
			String inputBoundingQuery = conf
					.get(DBConfiguration.INPUT_BOUNDING_QUERY);
			logger.debug("About to set the params");
			DBQueryInputFormat.setInput(job, inputQuery, inputBoundingQuery,
					params);
			logger.debug("Set the params");

			job.setNumReduceTasks(0);

			try {
				// job.setJarByClass(Class.forName(conf.get(
				// org.apache.hadoop.mapred.lib.db.DBConfiguration.DRIVER_CLASS_PROPERTY)));
				logger.debug("OUTPUT format class is "
						+ job.getOutputFormatClass());

				/*
				 * org.apache.hadoop.mapreduce.OutputFormat<?, ?> output =
				 * ReflectionUtils.newInstance(job.getOutputFormatClass(),
				 * job.getConfiguration()); output.checkOutputSpecs(job);
				 */
				logger.debug("Class is "
						+ ReflectionUtils
								.newInstance(job.getOutputFormatClass(),
										job.getConfiguration()).getClass()
								.getName());
				job.waitForCompletion(false);
				if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO) != null) {
					generateHiveScript(conf, job, jobCounter);
					generatePigScript(conf, job);
				}

			}
			/*
			 * catch (HIHOException h) { h.printStackTrace(); }
			 */
			catch (Exception e) {
				e.printStackTrace();
			} catch (HIHOException e) {
				e.printStackTrace();
			}
		}
		// avro to be handled differently, thanks to all the incompatibilities
		// in the apis.
		else {
			String inputQuery = conf.get(DBConfiguration.INPUT_QUERY);
			String inputBoundingQuery = conf
					.get(DBConfiguration.INPUT_BOUNDING_QUERY);
			logger.debug("About to set the params");
			// co.nubetech.apache.hadoop.mapred.DBQueryInputFormat.setInput(job,
			// inputQuery, inputBoundingQuery, params);
			logger.debug("Set the params");

			JobConf jobConf = new JobConf(conf);

			try {
				GenericDBWritable queryWritable = getDBWritable(jobConf);
				Schema pair = DBMapper
						.getPairSchema(queryWritable.getColumns());

				AvroJob.setMapOutputSchema(jobConf, pair);
				GenericRecordAvroOutputFormat.setOutputPath(jobConf, new Path(
						getConf().get(HIHOConf.INPUT_OUTPUT_PATH)));

				co.nubetech.apache.hadoop.mapred.DBQueryInputFormat.setInput(
						jobConf, inputQuery, inputBoundingQuery, params);
				jobConf.setInputFormat(co.nubetech.apache.hadoop.mapred.DBQueryInputFormat.class);
				jobConf.setMapperClass(DBInputAvroMapper.class);
				jobConf.setMapOutputKeyClass(NullWritable.class);
				jobConf.setMapOutputValueClass(AvroValue.class);
				jobConf.setOutputKeyClass(NullWritable.class);
				jobConf.setOutputValueClass(Text.class);
				jobConf.setOutputFormat(GenericRecordAvroOutputFormat.class);
				jobConf.setJarByClass(DBQueryInputJob.class);
				jobConf.setStrings(
						"io.serializations",
						"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization,org.apache.avro.mapred.AvroSerialization");
				jobConf.setNumReduceTasks(0);
				/*
				 * jobConf.setOutputFormat(org.apache.hadoop.mapred.
				 * SequenceFileOutputFormat.class);
				 * org.apache.hadoop.mapred.SequenceFileOutputFormat
				 * .setOutputPath(jobConf, new
				 * Path(getConf().get(HIHOConf.INPUT_OUTPUT_PATH)));
				 */
				JobClient.runJob(jobConf);
			} catch (Throwable e) {
				e.printStackTrace();
			}

		}

	}

}
