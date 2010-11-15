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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.log4j.Logger;

/**
 * A RecordReader that reads records from a MySQL table via
 * DataDrivenDBRecordReader
 */
public class MySQLQueryRecordReader extends DBQueryRecordReader {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.db.MySQLQueryRecordReader.class);

	public MySQLQueryRecordReader(DBInputFormat.DBInputSplit split,
			Configuration conf, Connection conn, DBConfiguration dbConfig,
			String cond, String[] fields, String table, ArrayList params)
			throws SQLException {
		super(split, conf, conn, dbConfig, cond, fields, table, "MYSQL", params);
	}

	protected ResultSet executeQuery(String query) throws SQLException {
		this.statement = getConnection().prepareStatement(query,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		if (params != null) {
			logger.debug("Params are to be set");
			for (int i = 0; i < params.size(); ++i) {
				statement.setObject(i + 1, params.get(i));
				logger.debug("iSet at " + i + " " + params.get(i));
			}
		}
		return statement.executeQuery();
	}
}
