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
package co.nubetech.hiho.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;

public class MySQLLoadDataMapper extends
		Mapper<NullWritable, FSDataInputStream, NullWritable, NullWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.MySQLLoadDataMapper.class);
	private Connection conn;
	
	public void setConnection(Connection con) throws IOException{
		conn=con;
	}
	
	public Connection getConnection(){
		return conn;
	}
	

	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			String connString = context.getConfiguration().get(
					DBConfiguration.URL_PROPERTY);
			String username = context.getConfiguration().get(
					DBConfiguration.USERNAME_PROPERTY);
			String password = context.getConfiguration().get(
					DBConfiguration.PASSWORD_PROPERTY);

			logger.debug("Connection values are " + connString + " " + username
					+ "/" + password);
			conn = DriverManager.getConnection(connString, username, password);

		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

	public void map(NullWritable key, FSDataInputStream val, Context context)
			throws IOException, InterruptedException {
          
		conn=getConnection();
		com.mysql.jdbc.Statement stmt = null;
		String query;
		try {

			stmt = (com.mysql.jdbc.Statement) conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
			stmt.setLocalInfileInputStream(val);
			query = "load data local infile 'abc.txt' into table ";
			query += context.getConfiguration().get(HIHOConf.LOAD_QUERY_SUFFIX);
			// query += "mrTest fields terminated by ','";
			stmt.executeUpdate(query);

		} catch (Exception e) {
			e.printStackTrace();
			stmt = null;
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException s) {
				s.printStackTrace();
			}
		}
	}

	protected void cleanup(Mapper.Context context) throws IOException,
			InterruptedException {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException s) {
			s.printStackTrace();
		}
	}

}
