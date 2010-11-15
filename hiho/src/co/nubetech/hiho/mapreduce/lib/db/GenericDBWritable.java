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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import co.nubetech.apache.hadoop.*;
import org.apache.log4j.Logger;

public class GenericDBWritable implements DBWritable {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable.class);

	private ArrayList<ColumnInfo> columns;
	private ArrayList values;

	public GenericDBWritable() {
	}

	public GenericDBWritable(ArrayList<ColumnInfo> meta, ArrayList values) {
		this.columns = meta;
		this.values = values;
	}

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

	/**
	 * Sets the fields of the object in the {@link PreparedStatement}.
	 * 
	 * @param statement
	 *            the statement that the fields are put into.
	 * @throws SQLException
	 */
	public void write(PreparedStatement statement) throws SQLException {
		for (int i = 0; i < values.size(); ++i) {
			statement.setObject(i + 1, values.get(i));
		}
	}

	public void setColumns(ArrayList<ColumnInfo> cols) {
		this.columns = cols;
	}

	public ArrayList<ColumnInfo> getColumns() {
		return columns;
	}

	/**
	 * Reads the fields of the object from the {@link ResultSet}.
	 * 
	 * @param resultSet
	 *            the {@link ResultSet} to get the fields from.
	 * @throws SQLException
	 */
	public void readFields(ResultSet resultSet) throws SQLException {
		values = new ArrayList();
		for (int i = 0; i < columns.size(); ++i) {
			values.add(resultSet.getObject(i + 1));
			logger.debug("Defalyed resultset to " + values.get(i));
		}
	}

	public ArrayList getValues() {
		return values;
	}

}
