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
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;
import co.nubetech.apache.hadoop.*;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class DelimitedLoadMapper<K, V> extends
		Mapper<K, V, GenericDBWritable, GenericDBWritable> {

	private PreparedStatement stmt;
	private Connection conn;
	private ResultSetMetaData meta;
	private ArrayList<ColumnInfo> columnInfo;
	private ArrayList values;

	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {
		try {

			String dbDriver = context.getConfiguration().get(
					DBConfiguration.DRIVER_CLASS_PROPERTY);
			String connString = context.getConfiguration().get(
					DBConfiguration.URL_PROPERTY);
			String username = context.getConfiguration().get(
					DBConfiguration.USERNAME_PROPERTY);
			String password = context.getConfiguration().get(
					DBConfiguration.PASSWORD_PROPERTY);
			String columnNames = context.getConfiguration().get(
					DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY);
			String tableName = context.getConfiguration().get(
					DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);

			Class.forName(dbDriver).newInstance();
			conn = DriverManager.getConnection(connString, username, password);

			String query = "select " + columnNames + " from " + tableName;
			PreparedStatement stmt = conn.prepareStatement(query);
			meta = stmt.getMetaData();

		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	public void map(K key, V val, Context context) throws IOException,
			InterruptedException {
		values = new ArrayList();
		try {
			columnInfo = GenericDBWritable.populateColumnInfo(meta);
		} catch (SQLException e) {

			e.printStackTrace();
		}

		StringTokenizer st = new StringTokenizer(val.toString(), ",");
		int j = st.countTokens();
		/*
		 * Since in our database data types of respective column are int,
		 * varchar ,varchar ,float
		 */
		for (int i = 0; i < columnInfo.size(); i++) {
			String s = st.nextToken();
			if (i == 0)
				values.add(Integer.parseInt(s));
			if (i == 1 || i == 2)
				values.add(s);
			if (i == 3)
				values.add(Float.valueOf(s));
		}

		GenericDBWritable gdw = new GenericDBWritable(columnInfo, values);

		context.write(gdw, gdw);

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
