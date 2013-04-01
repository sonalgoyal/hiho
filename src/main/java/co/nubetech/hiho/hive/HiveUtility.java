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
package co.nubetech.hiho.hive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import co.nubetech.hiho.mapreduce.lib.db.apache.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.OutputStrategyEnum;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class HiveUtility {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.hive.HiveUtility.class);

	public static void createTable(Configuration conf, Job job,
			GenericDBWritable writable, int jobCounter) throws HIHOException {
		String createQuery = null;
		String loadQuery = null;
		String tableName = getTableName(conf);
		FileOutputStream fos = null;
		boolean isCreateTable = true;
		boolean isDyanamicPartition = false;
		if (jobCounter > 0) {
			isCreateTable = false;
		}

		try {

			fos = getFileOutputStream(conf, jobCounter, tableName);
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(fos));

			String partitionBy = conf.get(HIHOConf.HIVE_PARTITION_BY);
			// tableName = getTableName(conf);

			if (conf.get(HIHOConf.HIVE_PARTITION_BY) == null) {

				if (isCreateTable) {
					createQuery = getCreateQuery(conf, writable);
					w.append(createQuery + "\n");
				}
				loadQuery = getLoadQuery(conf,
						conf.get(HIHOConf.INPUT_OUTPUT_PATH), writable);
				w.append(loadQuery + "\n");
				logger.warn("\nThe queries are: " + createQuery + "\n"
						+ loadQuery);
			} else {
				loadQuery = getLoadQuery(conf,
						conf.get(HIHOConf.INPUT_OUTPUT_PATH), writable,
						partitionBy);
				if (isCreateTable) {
					createQuery = getCreateQuery(conf, writable, partitionBy);
					w.append(createQuery + "\n");
				}
				w.append(loadQuery + "\n");
				logger.warn("\nThe queries are: " + createQuery + "\n"
						+ loadQuery);
				if (getDynamicPartitionBy(partitionBy) != null) {
					isDyanamicPartition = true;
				}
			}

			if (!isDyanamicPartition) {
				runQuery(createQuery, loadQuery, isCreateTable, conf);
			} else {

				String insertQuery = getInsertQueryFromTmpToMain(conf,
						writable, partitionBy);
				String tmpCreateQuery = getTmpCreateQuery(conf, writable);
				runQuery(tmpCreateQuery, insertQuery, createQuery, loadQuery,
						conf, isCreateTable);
				w.append(tmpCreateQuery + "\n" + loadQuery + "\n");
				if (isCreateTable) {
					w.append(createQuery + "\n");
				}
				w.append(insertQuery + "\n");
				logger.warn("\nThe queries are: " + createQuery + "\n"
						+ loadQuery + "\n" + tmpCreateQuery + "\n"
						+ insertQuery);
			}
			// w.append(createQuery + "\n" + loadQuery);
			w.close();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new HIHOException("Could not generate hive table", e);
		}
	}

	// This function is used to get the FileOutputStream for storing script of
	// hive queries
	public static FileOutputStream getFileOutputStream(Configuration conf,
			int jobCounter, String tableName) throws HIHOException {
		FileOutputStream fos = null;
		try {
			if (jobCounter == 0) {
				File file = new File(new File(
						conf.get(HIHOConf.INPUT_OUTPUT_LOADTO_PATH)),
						"hiveScript" + tableName + ".txt");

				fos = new FileOutputStream(file);

			} else {
				String path = conf.get(HIHOConf.INPUT_OUTPUT_LOADTO_PATH)
						+ "hiveScript" + tableName + ".txt";
				fos = new FileOutputStream(path, true);
			}
		} catch (FileNotFoundException e) {
			throw new HIHOException();
		}
		return fos;
	}

	// This function is used to get tableName for table we going to create in
	// Hive
	public static String getTableName(Configuration conf) throws HIHOException {
		String tableName = conf.get(HIHOConf.HIVE_TABLE_NAME);
		// if user did not specify hive table name, lets try to deduce it
		// automatically
		// get it from the name of the table we are querying against
		if (tableName == null) {
			tableName = conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
		}
		if (tableName == null) {
			if (conf.get(DBConfiguration.INPUT_QUERY) != null) {
				String query = conf.get(DBConfiguration.INPUT_QUERY);
				StringTokenizer queryTokens = new StringTokenizer(query, " ");
				while (queryTokens.hasMoreTokens()) {
					if (queryTokens.nextToken().equalsIgnoreCase("FROM")) {
						tableName = queryTokens.nextToken();
					}
				}
			}
		}

		// we are missing a couple of cases here. FROM table1, table2. FROM
		// table1 inner join blah blah
		// lets leave them for now and address them when the user need is
		// clearer

		/*
		 * if (conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY) != null) {
		 * tableName = conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY); }
		 * else if (conf.get(DBConfiguration.INPUT_QUERY) != null) { String
		 * query = conf.get(DBConfiguration.INPUT_QUERY); StringTokenizer
		 * queryTokens = new StringTokenizer(query, " "); while
		 * (queryTokens.hasMoreTokens()) { if
		 * (queryTokens.nextToken().equalsIgnoreCase("FROM")) { tableName =
		 * queryTokens.nextToken(); } } } if
		 * (conf.get(HIHOConf.HIVE_MULTIPLE_PARTITION_BY) != null &&
		 * conf.get(HIHOConf.HIVE_TABLE_NAME) != null) { tableName =
		 * conf.get(HIHOConf.HIVE_TABLE_NAME); }
		 */
		if (tableName == null) {
			throw new HIHOException("Cannot get hive table name");
		}
		return tableName;
	}

	/*
	 * This function is used to generate insertQuery which loads data from one
	 * table to another table in Hive, in case of dynamicPartition we create
	 * temporary table to load data then we transfer that data to our main table
	 * through this query
	 */
	public static String getInsertQueryFromTmpToMain(Configuration conf,
			GenericDBWritable writable, String partitionBy)
			throws HIHOException {
		StringBuilder builder = new StringBuilder();
		String tableName = getTableName(conf);
		builder.append("FROM `" + tableName
				+ "tmp` tmp INSERT OVERWRITE TABLE `" + tableName
				+ "` PARTITION (");
		// Here we are computing columnName and its value for partition,in
		// format columnName='columnValue',columnName...
		StringTokenizer tempToken = new StringTokenizer(partitionBy, ",");
		String columnsData = " ";
		while (tempToken.hasMoreTokens()) {
			StringTokenizer partitionData = new StringTokenizer(
					tempToken.nextToken(), ":");
			columnsData = columnsData + partitionData.nextToken();
			partitionData.nextToken();
			if (partitionData.hasMoreTokens()) {
				columnsData = columnsData + "='" + partitionData.nextToken()
						+ "'";
			}
			if (tempToken.hasMoreTokens()) {
				columnsData = columnsData + ",";
			}
		}
		//
		builder.append(columnsData + ") SELECT " + getTmpTableColumns(writable));
		return builder.toString();
	}

	// This function is used to get columns name from which we want to insert
	// values in our table columns,in format
	// `tableAlias`.`columnName`,`tableAlias`.`columnName`...
	public static String getTmpTableColumns(GenericDBWritable writable)
			throws HIHOException {
		StringBuilder builder = new StringBuilder();
		ArrayList<ColumnInfo> columns = writable.getColumns();
		for (ColumnInfo column : columns) {
			builder.append("`tmp`");
			builder.append(".`" + column.getName());
			builder.append("`,");
		}
		// above loop adds extra comma, removing.
		if (builder.length() > 0) {
			builder.deleteCharAt(builder.lastIndexOf(","));
		}
		return builder.toString();
	}

	// This function is used to get query for temporary table, in case of
	// dynamicPartition
	public static String getTmpCreateQuery(Configuration conf,
			GenericDBWritable writable) throws HIHOException {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE `" + getTableName(conf) + "tmp` ( ");

		builder.append(getColumns(writable));

		builder.append(") ROW FORMAT ");
		OutputStrategyEnum outputStrategy = OutputStrategyEnum.valueOf(conf
				.get(HIHOConf.INPUT_OUTPUT_STRATEGY));
		if (outputStrategy == OutputStrategyEnum.DELIMITED) {
			builder.append("DELIMITED FIELDS TERMINATED BY '");
			builder.append(conf.get(HIHOConf.INPUT_OUTPUT_DELIMITER));
			builder.append("' STORED AS TEXTFILE");
		}

		return builder.toString();
	}

	// This function is used return columnNames and columnType for partitionBy
	// clause in createTable query in right format.It takes data in format
	// columnName:columnType:[columnValue],columnName:columnType:[columnValue]...
	// and return in format columnName columnValue,columnName columnValue...
	public static String getPartitionBy(String partitionBy) {
		StringTokenizer partitionToken = new StringTokenizer(partitionBy, ";");

		String columnsData = "";
		while (partitionToken.hasMoreTokens()) {
			StringTokenizer partitionData = new StringTokenizer(
					partitionToken.nextToken(), ":");
			columnsData = columnsData + partitionData.nextToken() + " "
					+ partitionData.nextToken();
			if (partitionToken.hasMoreTokens()) {
				columnsData = columnsData + ",";
			}
		}
		return columnsData;
	}

	public static String getColumnsForPartitionedCreateTables(
			Configuration conf, String columns) {
		// columns format columnsName columnType,columnsName1 columnType1
		// get the column by which we want to do partition by
		// and remove that from the table creation
		String dynamicPartitionBy = getDynamicPartitionBy(conf
				.get(HIHOConf.HIVE_PARTITION_BY));
		StringTokenizer columnTokens = new StringTokenizer(columns, ",");
		columns = " ";
		int count = 0;
		while (columnTokens.hasMoreTokens()) {
			String columnData = columnTokens.nextToken();
			StringTokenizer columnDataTokens = new StringTokenizer(columnData,
					"` ");
			String columnName = columnDataTokens.nextToken();
			if (!columnName.equals(dynamicPartitionBy) && count > 0) {
				columns = columns + ",";
			}
			if (!columnName.equals(dynamicPartitionBy)) {
				columns = columns + columnData;
				count++;
			}

		}
		return columns;
	}

	// This function is used to get create table query in the case where user
	// has defined values in partitioned by clause
	public static String getCreateQuery(Configuration conf,
			GenericDBWritable writable, String partitionBy)
			throws HIHOException {

		StringBuilder builder = new StringBuilder();
		String tableName = getTableName(conf);
		if (conf.get(HIHOConf.HIVE_TABLE_OVERWRITE, "false").equals("true")) {
			builder.append("CREATE TABLE IF NOT EXISTS `" + tableName + "` (");
		} else {
			builder.append("CREATE TABLE `" + tableName + "` (");
		}
		String columns = getColumns(writable);

		builder.append(getColumnsForPartitionedCreateTables(conf, columns));
		builder.append(") PARTITIONED BY (" + getPartitionBy(partitionBy));
		builder.append(")");

		builder = appendClusteredByToCreateQuery(conf, builder);

		builder.append(" ROW FORMAT ");
		builder = appendDelimitedDataToCreateQuery(conf, builder);

		return builder.toString();

	}

	// This function is used to get create table query in the case where user
	// has not defined values in partitioned by clause
	public static String getCreateQuery(Configuration conf,
			GenericDBWritable writable) throws HIHOException {
		StringBuilder builder = new StringBuilder();
		String tableName = getTableName(conf);
		if (conf.get(HIHOConf.HIVE_TABLE_OVERWRITE, "false").equals("true")) {
			builder.append("CREATE TABLE IF NOT EXISTS `" + tableName + "` ( ");
		} else {
			builder.append("CREATE TABLE `" + tableName + "` ( ");
		}
		builder.append(getColumns(writable));
		builder.append(")");
		builder = appendClusteredByToCreateQuery(conf, builder);
		builder.append(" ROW FORMAT ");
		builder = appendDelimitedDataToCreateQuery(conf, builder);

		return builder.toString();
	}

	// //
	public static StringBuilder appendDelimitedDataToCreateQuery(
			Configuration conf, StringBuilder builder) {
		OutputStrategyEnum outputStrategy = OutputStrategyEnum.valueOf(conf
				.get(HIHOConf.INPUT_OUTPUT_STRATEGY));
		if (outputStrategy == OutputStrategyEnum.DELIMITED) {
			builder.append("DELIMITED FIELDS TERMINATED BY '");
			builder.append(conf.get(HIHOConf.INPUT_OUTPUT_DELIMITER));
			builder.append("' STORED AS TEXTFILE");
		}
		return builder;
	}

	public static StringBuilder appendClusteredByToCreateQuery(
			Configuration conf, StringBuilder builder) throws HIHOException {
		if (conf.get(HIHOConf.HIVE_CLUSTERED_BY) != null) {
			StringTokenizer clusterData = new StringTokenizer(
					conf.get(HIHOConf.HIVE_CLUSTERED_BY), ":");
			builder.append(" CLUSTERED BY (" + clusterData.nextToken());
			builder.append(")");
			if (conf.get(HIHOConf.HIVE_SORTED_BY) != null) {
				builder.append(" SORTED BY ("
						+ conf.get(HIHOConf.HIVE_SORTED_BY));
				builder.append(")");
			}
			try {
				String buckets = clusterData.nextToken();
				if (buckets == null) {
					throw new HIHOException(
							"The number of buckets wih clustered by is not defined");

				}
				builder.append(" INTO " + buckets + " BUCKETS");
			} catch (NoSuchElementException e) {
				throw new HIHOException(
						"The number of buckets wih clustered by is not defined");

			}

		}

		return builder;

	}

	/*
	 * Parses the partition by configuration of the form
	 * col:type[:value],col1:type1[:value1] and provides the name of the dynamic
	 * partition
	 */

	public static String getDynamicPartitionBy(String partitionBy) {
		StringTokenizer partitionToken = new StringTokenizer(partitionBy, ";");
		String dynamicPartitionBy = null;
		while (partitionToken.hasMoreTokens()) {

			StringTokenizer columnTokens = new StringTokenizer(
					partitionToken.nextToken(), ":");
			String columnNames = columnTokens.nextToken();
			columnTokens.nextToken();
			if (!columnTokens.hasMoreTokens()) {
				dynamicPartitionBy = columnNames;
			}
		}
		return dynamicPartitionBy;
	}

	public static String getLoadQuery(Configuration conf, String hdfsDir,
			GenericDBWritable writable, String partitionBy)
			throws HIHOException {
		String loadQuery = getLoadQuery(conf, hdfsDir, writable);
		StringTokenizer tempToken = new StringTokenizer(partitionBy, ";");

		String columnsData = " ";
		while (tempToken.hasMoreTokens()) {
			StringTokenizer columnTokens = new StringTokenizer(
					tempToken.nextToken(), ":");
			String columnNames = columnTokens.nextToken();
			columnsData = columnsData + columnNames;
			columnTokens.nextToken();
			if (columnTokens.hasMoreTokens()) {
				columnsData = columnsData + "='" + columnTokens.nextToken()
						+ "'";
			}
			if (tempToken.hasMoreTokens()) {
				columnsData = columnsData + ",";
			}
		}
		if (getDynamicPartitionBy(conf.get(HIHOConf.HIVE_PARTITION_BY)) == null) {
			loadQuery = loadQuery + "` PARTITION (" + columnsData + ")";
		} else {
			loadQuery = loadQuery + "tmp`";
		}

		return loadQuery;
	}

	public static String getLoadQuery(Configuration conf, String hdfsDir,
			GenericDBWritable writable) throws HIHOException {
		StringBuilder builder = new StringBuilder();

		builder.append("LOAD DATA INPATH '");
		builder.append(hdfsDir);
		builder.append("' OVERWRITE INTO TABLE `");
		builder.append(getTableName(conf));
		if (conf.get(HIHOConf.HIVE_PARTITION_BY) == null) {
			builder.append("`");
		}
		return builder.toString();
	}

	// //Accept String query to be run

	public static void runQuery(String createQuery, String loadQuery,
			boolean createTable, Configuration conf) throws HIHOException {
		try {
			Class.forName(conf.get(HIHOConf.HIVE_DRIVER));
			Connection con = DriverManager.getConnection(
					conf.get(HIHOConf.HIVE_URL),
					conf.get(HIHOConf.HIVE_USR_NAME),
					conf.get(HIHOConf.HIVE_PASSWORD));
			Statement stmt = con.createStatement();
			// stmt.executeQuery("drop table " + tableName);
			if (createTable) {
				stmt.executeQuery("drop table " + getTableName(conf));
				stmt.executeQuery(createQuery);
			}
			stmt.executeQuery(loadQuery);

			/*
			 * ResultSet rs = stmt.executeQuery("select * from " + tableName);
			 * while (rs.next()) { System.out.println(rs.getString(1) + "\t" +
			 * rs.getString(2)); }
			 */

			stmt.close();
			con.close();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static void runQuery(String tmpCreateQuery, String insertQuery,
			String createQuery, String loadQuery, Configuration conf,
			boolean createTable) throws HIHOException {
		try {
			Class.forName(conf.get(HIHOConf.HIVE_DRIVER));
			Connection con = DriverManager.getConnection(
					conf.get(HIHOConf.HIVE_URL),
					conf.get(HIHOConf.HIVE_USR_NAME),
					conf.get(HIHOConf.HIVE_PASSWORD));
			Statement stmt = con.createStatement();
			stmt.executeQuery("drop table " + getTableName(conf) + "tmp");
			stmt.executeQuery("drop table " + getTableName(conf));
			stmt.executeQuery(tmpCreateQuery);
			stmt.executeQuery(loadQuery);
			/*
			 * ResultSet rs2 = stmt.executeQuery("select * from " + tableName +
			 * "tmp"); while (rs2.next()) { System.out.println(rs2.getString(1)
			 * + "\t" + rs2.getString(2)); }
			 */
			if (createTable) {
				stmt.executeQuery(createQuery);
			}
			stmt.executeQuery(insertQuery);
			stmt.executeQuery("drop table " + getTableName(conf) + "tmp");

			stmt.close();
			con.close();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static String getColumns(GenericDBWritable writable)
			throws HIHOException {
		StringBuilder builder = new StringBuilder();
		ArrayList<ColumnInfo> columns = writable.getColumns();
		for (ColumnInfo column : columns) {
			builder.append("`");
			builder.append(column.getName());
			builder.append("` ");
			builder.append(getColumnType(column.getType()));
			builder.append(",");
		}

		if (builder.length() > 0) {
			builder.deleteCharAt(builder.lastIndexOf(","));
		}
		return builder.toString();
	}

	// //Column Types to be verified and junits added
	public static String getColumnType(int columnType) throws HIHOException {
		String returnType = null;
		switch (columnType) {

		case Types.BIGINT:
			returnType = "long";
			break;
		case Types.BOOLEAN:
		case Types.CHAR:
			returnType = "string";
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
			returnType = "string";
			break;
		default:
			throw new HIHOException("Unsupported type");
		}
		return returnType;
	}

}
