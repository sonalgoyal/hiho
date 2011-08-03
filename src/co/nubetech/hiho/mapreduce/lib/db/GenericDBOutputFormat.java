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
package co.nubetech.hiho.mapreduce.lib.db;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.apache.hadoop.DBOutputFormat;
import co.nubetech.hiho.common.HIHOConf;

public class GenericDBOutputFormat extends DBOutputFormat {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.db.GenericDBOutputFormat.class);

	public static ArrayList<ColumnInfo> populateColumnInfo(
			ResultSetMetaData meta) throws SQLException {
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();

		for (int i = 0; i < meta.getColumnCount(); ++i) {
			ColumnInfo info = new ColumnInfo(i, meta.getColumnType(i + 1),
					meta.getColumnName(i + 1));
			columns.add(info);
		}
		return columns;
	}

	public static void setOutput(Job job, String tableName, String columnNames)
			throws IOException{
		job.setOutputFormatClass(GenericDBOutputFormat.class);
		DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
		dbConf.setOutputTableName(tableName);
		dbConf.setOutputFieldNames(columnNames);

		String dbDriver = job.getConfiguration().get(
				DBConfiguration.DRIVER_CLASS_PROPERTY);
		String connString = job.getConfiguration().get(
				DBConfiguration.URL_PROPERTY);
		String username = job.getConfiguration().get(
				DBConfiguration.USERNAME_PROPERTY);
		String password = job.getConfiguration().get(
				DBConfiguration.PASSWORD_PROPERTY);
		
		Connection conn;
		PreparedStatement stmt;
		try {
			Class.forName(dbDriver).newInstance();
			conn = DriverManager.getConnection(connString, username,	password);
			String query = "select " + columnNames + " from " + tableName;
			stmt = conn.prepareStatement(query);
			ResultSetMetaData meta = stmt.getMetaData();			
			ArrayList<ColumnInfo> columnInfo = populateColumnInfo(meta);			
			String jsonString = getJsonStringOfColumnInfo(columnInfo);
			job.getConfiguration().set(HIHOConf.COLUMN_INFO, jsonString);		
			logger.debug("columnInfo is: " + job.getConfiguration().get(HIHOConf.COLUMN_INFO));			
			stmt.close();
			conn.close();			
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}	
		
	}

	public static String getJsonStringOfColumnInfo(ArrayList<ColumnInfo> columnInfo) throws IOException {
		ObjectMapper mapper = new ObjectMapper();			
		JsonFactory jsonFactory = new JsonFactory();	
		StringWriter writer = new StringWriter();
		JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
		mapper.writeValue(jsonGenerator, columnInfo);
		String jsonString = writer.toString();
		return jsonString;
	}

}
