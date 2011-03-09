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
package co.nubetech.hiho.pig;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.pig.backend.executionengine.ExecException;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

/**
 * Generic bunch of utilities which can be used to create Pig schema
 * 
 * @author sgoyal
 * 
 */
public class PigUtility {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.pig.PigUtility.class);

	/**
	 * Returns the load script for a hdfs folder and a jdbc query result set
	 * metadata
	 * 
	 * @param hdfsDir
	 * @param writable
	 * @return
	 * @throws IOException
	 * @throws ExecException
	 */
	public static String getLoadScript(String hdfsDir,
			GenericDBWritable writable) throws HIHOException {
		StringBuilder builder = new StringBuilder();
		builder.append("A = LOAD '");
		builder.append(hdfsDir);
		builder.append("' USING PigStorage(',') AS (");
		builder.append(getColumns(writable));
		builder.append(");");
		logger.info("pig load script : " + builder.toString());
		return builder.toString();
	}

	public static String getColumns(GenericDBWritable writable)
			throws HIHOException {
		StringBuilder builder = new StringBuilder();
		ArrayList<ColumnInfo> columns = writable.getColumns();
		for (ColumnInfo column : columns) {
			builder.append(column.getName());
			builder.append(":");
			builder.append(getColumnType(column.getType()));
			builder.append(",");
		}
		// above loop adds extra comma, removing.
		if (builder.length() > 0) {
			builder.deleteCharAt(builder.lastIndexOf(","));
		}
		return builder.toString();
	}

	public static String getColumnType(int columnType) throws HIHOException {
		String returnType = null;
		switch (columnType) {

		case Types.BIGINT:
			returnType = "long";
			break;
		case Types.BOOLEAN:
		case Types.CHAR:
			returnType = "chararray";
			break;
		case Types.DECIMAL:
		case Types.REAL:
		case Types.NUMERIC:
		case Types.DOUBLE:
			returnType = "double";
			break;
		case Types.FLOAT:
			returnType = "float";
			break;
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			returnType = "int";
			break;
		case Types.BINARY:
		case Types.CLOB:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			returnType = "bytearray";
			break;
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			returnType = "chararray";
			break;
		default:
			throw new HIHOException("Unsupported type");
		}
		return returnType;
	}
}
