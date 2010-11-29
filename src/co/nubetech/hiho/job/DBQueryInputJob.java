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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import co.nubetech.apache.hadoop.*;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;




import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.OutputStrategyEnum;
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

	@Override
	@SuppressWarnings("unchecked")
	public int run(String[] args) throws IOException {
		Configuration conf = getConf();

		Job job = new Job(conf);
		for (Entry<String, String> entry : conf) {
			logger.debug("key, value " + entry.getKey() + "="
					+ entry.getValue());
		}
		//logger.debug("Number of maps " + conf.getInt("mapred.map.tasks", 1));
		//conf.setInt(JobContext.NUM_MAPS, conf.getInt("mapreduce.job.maps", 1));
		//job.getConfiguration().setInt("mapred.map.tasks", 4);
		job.getConfiguration().setInt(MRJobConfig.NUM_MAPS, conf.getInt(HIHOConf.NUMBER_MAPPERS, 1)); 
		logger.debug("Number of maps " + conf.getInt(MRJobConfig.NUM_MAPS, 1));
		
		job.setJobName("Import job");
		job.setJarByClass(DBQueryInputJob.class);
		
		String strategy = conf.get(HIHOConf.INPUT_OUTPUT_STRATEGY);
		OutputStrategyEnum os = OutputStrategyEnum.valueOf(strategy);
		if (os == null) {
			throw new IllegalArgumentException(
					"Wrong value of output strategy. Please correct");
		}
		switch (os) {

			case DUMP: {
				//job.setMapperClass(DBImportMapper.class);
				break;
			}
			case AVRO: {
				job.setMapperClass(DBInputAvroMapper.class);
				//need avro in cp
				//job.setJarByClass(Schema.class);
				//need jackson which is needed by avro - ugly!
				//job.setJarByClass(ObjectMapper.class);
				job.setMapOutputKeyClass(GenericRecord.class);
				job.setMapOutputValueClass(Text.class);
				break;
			}
			case DELIMITED: {
				job.setMapperClass(DBInputDelimMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				break;
			}
			case JSON: {
				//job.setMapperClass(DBImportJsonMapper.class);
				//job.setJarByClass(ObjectMapper.class);
				break;
			}
			default: {
				job.setMapperClass(DBInputDelimMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				break;
			}
		}
		
		String inputQuery = conf.get(DBConfiguration.INPUT_QUERY);
		String inputBoundingQuery = conf.get(DBConfiguration.INPUT_BOUNDING_QUERY);
		logger.debug("About to set the params");
		DBQueryInputFormat.setInput(job, inputQuery, inputBoundingQuery, params);
		logger.debug("Set the params");
		
		job.setOutputFormatClass(NoKeyOnlyValueOutputFormat.class);

		NoKeyOnlyValueOutputFormat.setOutputPath(job,
				new Path(getConf().get(HIHOConf.INPUT_OUTPUT_PATH)));
		
		job.setNumReduceTasks(0);
		

		try {
			//job.setJarByClass(Class.forName(conf.get(
			//		org.apache.hadoop.mapred.lib.db.DBConfiguration.DRIVER_CLASS_PROPERTY)));
			logger.debug("Output format class is " + job.getOutputFormatClass());
			
			 /*org.apache.hadoop.mapreduce.OutputFormat<?, ?> output =
			        ReflectionUtils.newInstance(job.getOutputFormatClass(),
			          job.getConfiguration());
			      output.checkOutputSpecs(job);
			   */
			logger.debug("Class is " + ReflectionUtils.newInstance(job.getOutputFormatClass(), job.getConfiguration()).getClass().getName());
			job.waitForCompletion(false);
		
			//generatePigScript(conf, job);
		} 
		/*catch (HIHOException h) {
			h.printStackTrace();
		}*/
		catch(Exception e) {
			e.printStackTrace();
		}		
		return 0;
	}
	
	private void generatePigScript(Configuration conf,Job job) throws HIHOException {
		///see if import to pig or hive
		if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO).equals("pig")) {
			try {
				String pigScript = PigUtility.getLoadScript(HIHOConf.INPUT_OUTPUT_PATH, getDBWritable(conf));
				File file = new File(new File(conf.get(HIHOConf.INPUT_OUTPUT_LOADTO_PATH)), 
						"pigScript" + job.getJobID() + ".txt");
				FileOutputStream fos = new FileOutputStream(file);
		        BufferedWriter w = new BufferedWriter(new OutputStreamWriter(fos));
		        w.write(pigScript);
		        w.close();
		        fos.close();
			}
			catch (Exception h) {
				throw new HIHOException("Unable to generate Pig script", h);
			}
		}	

	}
	
	private void generateHiveScript(Configuration conf,Job job) throws HIHOException {
		///see if import to pig or hive
		if (conf.get(HIHOConf.INPUT_OUTPUT_LOADTO).equals("hive")) {
			try {
				//HiveUtility.createTable(conf, job, getDBWritable(conf));
			}
			catch (Exception h) {
				throw new HIHOException("Unable to generate Pig script", h);
			}
		}	

	}

	public static void main(String[] args) throws Exception {
		//setUp();
		DBQueryInputJob job = new DBQueryInputJob();
		//ArrayList params = new ArrayList();
		//params.add(false);
		//job.setParams(params);
		int res = ToolRunner.run(new Configuration(), job,
				args);
		System.exit(res);
	}

	
	
	
	public ArrayList getParams() {
		return params;
	}

	public void setParams(ArrayList params) {
		this.params = params;
	}

	/* this will move to the junit once everything is properly done

	public static void setUp() {
		// set up the database
		String db = "cumulus";
		String root = "root";
		String pwd = "newpwd";

		String user = "tester";
		String password = "password";

		Connection conn;
		String url = "jdbc:mysql://localhost:3306/";
		String driverName = "com.mysql.jdbc.Driver";

		try {
			Class.forName(driverName).newInstance();
			conn = DriverManager.getConnection(url, root, pwd);
			try {
				Statement st = conn.createStatement();

				String dbDrop = "drop database if exists " + db;
				st.executeUpdate(dbDrop);
				logger.debug("Dropped database");

				String dbCreate = "create database " + db;
				st.executeUpdate(dbCreate);
				logger.debug("Created database");

				// Register a new user named tester on the
				// database named cumulus with a password
				// password enabling several different
				// privileges.
				st.executeUpdate("GRANT SELECT,INSERT,UPDATE,DELETE,"
						+ "CREATE,DROP " + "ON " + db + ".* TO '" + user
						+ "'@'localhost' " + "IDENTIFIED BY '" + password
						+ "';");
				logger.debug("Created user tester");
				st.close();

				// now connect to the relevent db and create the schema
				conn = DriverManager.getConnection(url + db, root, pwd);
				st = conn.createStatement();

				String desTable = "Create table if not exists designations(id integer, designation varchar(30));\n";
				st = conn.createStatement();
				st.executeUpdate(desTable);

				logger.debug(desTable);

				String desTableData = "insert into designations(id, designation) values(";
				desTableData += "0, 'Manager');\n";
				st.executeUpdate(desTableData);
				desTableData = "insert into designations(id, designation) values(";
				desTableData += "1, 'Accountant');\n";
				st.executeUpdate(desTableData);
				desTableData = "insert into designations(id, designation) values(";
				desTableData += "2, 'Assistant');\n";
				st.executeUpdate(desTableData);
				desTableData = "insert into designations(id, designation) values(";
				desTableData += "3, 'Sr. Manager');\n";
				logger.debug(desTableData);
				st.executeUpdate(desTableData);
				logger.debug("Data inserte4d into designations");

				String table = "CREATE TABLE if not exists extractJobEmployee(id integer, name varchar(50), age integer";
				table += ", isMarried boolean, salary double, designationId integer);";
				st = conn.createStatement();
				st.executeUpdate(table);

				logger.debug("Schema creation process successfull!");
				logger.debug("Inserting table data");

				for (int i = 0; i < 50; ++i) {
					int designation = i % 4;
					String tableData = "Insert into extractJobEmployee(id, name, age, isMarried, salary, designationId) values(";
					tableData += i + ", 'Employee" + i;
					tableData += "', 25, false, 349.9," + designation + ");\n";
					logger.debug(tableData);
					st = conn.createStatement();
					st.executeUpdate(tableData);
				}
				// conn.commit();
				logger.debug("Table data insertion process successfull!");
			} catch (SQLException s) {
				s.printStackTrace();
			}
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	*/
	
	public static GenericDBWritable getDBWritable(Configuration conf) throws HIHOException {
		try {
			String driverName = conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY);
			String url = conf.get(DBConfiguration.URL_PROPERTY);
			String user = conf.get(DBConfiguration.USERNAME_PROPERTY);
			String password = conf.get(DBConfiguration.PASSWORD_PROPERTY);
			Class.forName(driverName).newInstance();
			Connection conn = DriverManager.getConnection(url, user, password);
			DatabaseMetaData dbMeta = conn.getMetaData();
		    String dbProductName = dbMeta.getDatabaseProductName().toUpperCase();
		    String query = getSelectQuery(conf, dbProductName);
			PreparedStatement st = conn.prepareStatement(query);
			GenericDBWritable writable = new GenericDBWritable(GenericDBWritable.populateColumnInfo(st.getMetaData()),
					null);
			return writable;
		    
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new HIHOException("Unable to get metadata for the query", e);
		}
	}
	
	/** Returns the query for selecting the records,
	   * subclasses can override this for custom behaviour.
	 A lot is actually a copy from DataDrivenDBRecordReader */
	  @SuppressWarnings("unchecked")
	  protected static String getSelectQuery(Configuration conf, String dbProductName) throws HIHOException{
		  
		  
	    StringBuilder query = new StringBuilder();
	    DBConfiguration dbConf = new DBConfiguration(conf);
	    String [] fieldNames = dbConf.getInputFieldNames();
	    String tableName = dbConf.getInputTableName();
	    String conditions = dbConf.getInputConditions();

	    // Build the WHERE clauses associated with the data split first.
	    // We need them in both branches of this function.
	    StringBuilder conditionClauses = new StringBuilder();
	    
	    if(dbConf.getInputQuery() == null) {
	      // We need to generate the entire query.
	      query.append("SELECT ");

	      for (int i = 0; i < fieldNames.length; i++) {
	        query.append(fieldNames[i]);
	        if (i != fieldNames.length -1) {
	          query.append(", ");
	        }
	      }

	      query.append(" FROM ").append(tableName);
	      if (!dbProductName.startsWith("ORACLE")) {
	        // Seems to be necessary for hsqldb? Oracle explicitly does *not*
	        // use this clause.
	        query.append(" AS ").append(tableName);
	      }
	      query.append(" WHERE ");
	      if (conditions != null && conditions.length() > 0) {
	        // Put the user's conditions first.
	        query.append("( ").append(conditions).append(" ) AND ");
	      }

	      // Now append the conditions associated with our split.
	      query.append(conditionClauses.toString());

	    } else {
	      // User provided the query. We replace the special token with our WHERE clause.
	      String inputQuery = dbConf.getInputQuery();
	      if (inputQuery.indexOf(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
	        logger.error("Could not find the clause substitution token "
	            + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN + " in the query: ["
	            + inputQuery + "]. Parallel splits may not work correctly.");
	        throw new HIHOException("Please check input query");
	      }

	      query.append(inputQuery.replace(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN,
	          conditionClauses.toString()));
	    }

	    logger.debug("Using query: " + query.toString());

	    return query.toString();
	  }

}
