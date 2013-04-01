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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import co.nubetech.hiho.mapreduce.lib.db.apache.DBConfiguration;
import co.nubetech.hiho.mapreduce.lib.db.apache.DBInputFormat;
import co.nubetech.hiho.mapreduce.lib.db.apache.DataDrivenDBRecordReader;

public class DBQueryRecordReader extends
		DataDrivenDBRecordReader<GenericDBWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.db.DBQueryRecordReader.class);

	protected ArrayList params;
	private LongWritable key = null;

	private GenericDBWritable value = null;
	private ArrayList<ColumnInfo> info;
	private ResultSet results;
	private long pos = 0;

	public ArrayList getParams() {
		return params;
	}

	public void setParams(ArrayList params) {
		this.params = params;
	}

	public DBQueryRecordReader(DBInputFormat.DBInputSplit split,
			Configuration conf, Connection conn, DBConfiguration dbConfig,
			String cond, String[] fields, String table, String dbProduct,
			ArrayList params) throws SQLException {
		super(split, GenericDBWritable.class, conf, conn, dbConfig, cond,
				fields, table, dbProduct);
		this.params = params;
	}

	protected ResultSet executeQuery(String query) throws SQLException {
		this.statement = getConnection().prepareStatement(query,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		if (params != null) {
			logger.debug("Params are to be set");
			for (int i = 0; i < params.size(); ++i) {
				statement.setObject(i + 1, params.get(i));
				logger.debug("set i " + i + " " + params.get(i));
			}
		}

		return statement.executeQuery();

	}

	/** {@inheritDoc} */
	public LongWritable getCurrentKey() {
		return key;
	}

	/** {@inheritDoc} */
	public GenericDBWritable getCurrentValue() {
		return value;
	}

	/** {@inheritDoc} */
	public boolean nextKeyValue() throws IOException {
		try {
			if (key == null) {
				key = new LongWritable();
			}
			if (value == null) {
				value = new GenericDBWritable();
			}
			if (null == this.results) {
				// First time into this method, run the query.
				this.results = executeQuery(getSelectQuery());
				info = GenericDBWritable.populateColumnInfo(results
						.getMetaData());
				logger.debug("Info is " + info);
			}
			if (!results.next()) {
				logger.debug("No results found ");
				return false;
			}
			// Set the key field value as the output key value
			key.set(pos + getSplit().getStart());
			value.setColumns(info);
			value.readFields(results);
			logger.debug("Set key, value");
			pos++;
		} catch (SQLException e) {
			throw new IOException("SQLException in nextKeyValue", e);
		}
		return true;
	}

}
