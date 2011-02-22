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

package co.nubetech.hiho.mapreduce.lib.db;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.apache.hadoop.DBInputFormat;
import co.nubetech.apache.hadoop.DataDrivenDBInputFormat;
import co.nubetech.hiho.common.HIHOConf;

public class DBQueryInputFormat extends
		DataDrivenDBInputFormat<GenericDBWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.db.DBQueryInputFormat.class);

	@Override
	protected RecordReader<LongWritable, GenericDBWritable> createDBRecordReader(
			DBInputSplit split, Configuration conf) throws IOException {

		DBConfiguration dbConf = getDBConf();
		@SuppressWarnings("unchecked")
		// Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
		String dbProductName = getDBProductName();

		logger.debug("Creating db record reader for db product: "
				+ dbProductName);
		ArrayList params = null;
		try {
			if (conf.get(HIHOConf.QUERY_PARAMS) != null) {
				logger.debug("creating stringifier in DBQueryInputFormat");
				DefaultStringifier<ArrayList> stringifier = new DefaultStringifier<ArrayList>(
						conf, ArrayList.class);
				logger.debug("created stringifier");

				params = stringifier
						.fromString(conf.get(HIHOConf.QUERY_PARAMS));
				logger.debug("created params");
			}
			// use database product name to determine appropriate record reader.
			if (dbProductName.startsWith("MYSQL")) {
				// use MySQL-specific db reader.
				return new MySQLQueryRecordReader(split, conf, getConnection(),
						dbConf, dbConf.getInputConditions(),
						dbConf.getInputFieldNames(),
						dbConf.getInputTableName(), params);
			} else {
				// Generic reader.
				return new DBQueryRecordReader(split, conf, getConnection(),
						dbConf, dbConf.getInputConditions(),
						dbConf.getInputFieldNames(),
						dbConf.getInputTableName(), dbProductName, params);
			}
		} catch (SQLException ex) {
			throw new IOException(ex.getMessage());
		}
	}

	// Configuration methods override superclass to ensure that the proper
	// DataDrivenDBInputFormat gets used.

	/**
	 * Note that the "orderBy" column is called the "splitBy" in this version.
	 * We reuse the same field, but it's not strictly ordering it -- just
	 * partitioning the results.
	 */
	public static void setInput(Job job, String tableName, String conditions,
			String splitBy, ArrayList params, String... fieldNames)
			throws IOException {
		DBInputFormat.setInput(job, GenericDBWritable.class, tableName,
				conditions, splitBy, fieldNames);
		if (params != null) {
			DefaultStringifier<ArrayList> stringifier = new DefaultStringifier<ArrayList>(
					job.getConfiguration(), ArrayList.class);
			job.getConfiguration().set(HIHOConf.QUERY_PARAMS,
					stringifier.toString(params));
			logger.debug("Converted params and saved them into config");
		}
		job.setInputFormatClass(DBQueryInputFormat.class);
	}

	/**
	 * setInput() takes a custom query and a separate "bounding query" to use
	 * instead of the custom "count query" used by DBInputFormat.
	 */
	public static void setInput(Job job, String inputQuery,
			String inputBoundingQuery, ArrayList params) throws IOException {
		DBInputFormat.setInput(job, GenericDBWritable.class, inputQuery, "");
		if (inputBoundingQuery != null) {
			job.getConfiguration().set(DBConfiguration.INPUT_BOUNDING_QUERY,
					inputBoundingQuery);
		}
		if (params != null) {
			DefaultStringifier<ArrayList> stringifier = new DefaultStringifier<ArrayList>(
					job.getConfiguration(), ArrayList.class);
			job.getConfiguration().set(HIHOConf.QUERY_PARAMS,
					stringifier.toString(params));
			logger.debug("Converted params and saved them into config");
		}
		job.setInputFormatClass(DBQueryInputFormat.class);
	}

}
