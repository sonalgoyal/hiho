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
package co.nubetech.hiho.hive;

import static org.junit.Assert.assertEquals;

import java.sql.Types;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import co.nubetech.hiho.mapreduce.lib.db.apache.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.job.DBQueryInputJob;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class TestHiveUtility {

	@Test
	public void testGetSelectQuery() throws HIHOException {
		Configuration conf = new Configuration();
		Configuration conf1 = new Configuration();
		conf.set(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, "id,name");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "student");
		// conf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY,"");
		// conf.set(DBConfiguration.INPUT_QUERY,"select * from student");
		conf1.set(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY,
				"empId,empName,empAddress");
		conf1.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "employee");
		// conf1.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY,"");
		conf1.set(DBConfiguration.INPUT_QUERY, "select * from employee");
		String dbProductName = "MYSQL";
		assertEquals("SELECT id, name FROM student AS student",
				DBQueryInputJob.getSelectQuery(conf, dbProductName));
		assertEquals("select * from employee",
				DBQueryInputJob.getSelectQuery(conf1, dbProductName));

	}

	@Test
	public void testGetTableName() throws HIHOException {
		Configuration conf = new Configuration();
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "student");
		conf.set(DBConfiguration.INPUT_QUERY, "select * from student");
		// conf.set(HIHOConf.HIVE_MULTIPLE_PARTITION_BY,
		// "country:string:us,ca");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "hive");
		conf1.set(DBConfiguration.INPUT_QUERY, "select * from student");
		conf2.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "employee");
		assertEquals("hive", HiveUtility.getTableName(conf));
		assertEquals("student", HiveUtility.getTableName(conf1));
		assertEquals("employee", HiveUtility.getTableName(conf2));
	}

	@Test
	public void testGetInsertQuery() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		// HiveUtility.tableName = "employee";
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		Configuration conf = new Configuration();
		conf.set(HIHOConf.HIVE_PARTITION_BY, "country:string:us,name:string");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "employee");
		assertEquals(
				"FROM `employeetmp` tmp INSERT OVERWRITE TABLE `employee` PARTITION ( country='us',name) SELECT `tmp`.`intColumn`,`tmp`.`stringColumn`",
				HiveUtility.getInsertQueryFromTmpToMain(conf, writable,
						conf.get(HIHOConf.HIVE_PARTITION_BY)));

	}

	@Test
	public void testGetTableColumns() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		assertEquals("`tmp`.`intColumn`,`tmp`.`stringColumn`",
				HiveUtility.getTmpTableColumns(writable));
	}

	@Test
	public void testGetTmpCreateQuery() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "employee");
		GenericDBWritable writable = new GenericDBWritable(columns, null);

		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		assertEquals(
				"CREATE TABLE `employeetmp` ( `intColumn` int,`stringColumn` string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
				HiveUtility.getTmpCreateQuery(conf, writable));
	}

	@Test
	public void testGetPartitionBy() {
		String partitionBy = "country:string:us;name:string:jack";
		assertEquals("country string,name string",
				HiveUtility.getPartitionBy(partitionBy));
	}

	@Test
	public void testGetColumnsForPartitionedCreateTables() {
		Configuration conf = new Configuration();
		conf.set(HIHOConf.HIVE_PARTITION_BY, "country:string:us;name:string:");
		String columns = "id int,name string";
		assertEquals(" id int",
				HiveUtility.getColumnsForPartitionedCreateTables(conf, columns));
	}

	// @Test(expected = HIHOException.class)
	@Test(expected = HIHOException.class)
	public void testGetCreateQuery() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "id");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR, "country");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn); // HiveUtility.// = "employee";
		GenericDBWritable writable = new GenericDBWritable(columns, null);

		// This is normal case to generate basic create query
		Configuration conf = new Configuration();
		conf.set(HIHOConf.HIVE_TABLE_NAME, "employee");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ";");
		assertEquals(
				"CREATE TABLE `employee` ( `id` int,`country` string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE",
				HiveUtility.getCreateQuery(conf, writable));

		// This case show create query when clusteredBy and sortedBy
		// configuration is given user,please make a note sortedBy feature will
		// not work till clusteredBy feature is not defined.
		Configuration conf1 = new Configuration();
		conf1.set(HIHOConf.HIVE_TABLE_NAME, "employee");
		conf1.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf1.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		conf1.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		conf1.set(HIHOConf.HIVE_CLUSTERED_BY, "name:2");
		conf1.set(HIHOConf.HIVE_SORTED_BY, "name");
		// System.out.println(HiveUtility.getCreateQuery(conf1, writable));
		assertEquals(
				"CREATE TABLE `employee` ( `id` int,`country` string) CLUSTERED BY (name) SORTED BY (name) INTO 2 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
				HiveUtility.getCreateQuery(conf1, writable));

		// This case is when user has defined partitionedBy configuration
		Configuration conf3 = new Configuration();
		conf3.set(HIHOConf.HIVE_TABLE_NAME, "employee");
		conf3.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf3.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		conf3.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		conf3.set(HIHOConf.HIVE_CLUSTERED_BY, "name:2");
		conf3.set(HIHOConf.HIVE_SORTED_BY, "name");
		conf3.set(HIHOConf.HIVE_PARTITION_BY, "country:string");
		String partitionBy = "country:string";
		// String partitionBy1 = "name:string:raj,country:string";
		// System.out.println(HiveUtility.getCreateQuery(conf3,
		// writable,partitionBy));
		assertEquals(
				"CREATE TABLE `employee` ( `id` int) PARTITIONED BY (country string) CLUSTERED BY (name) SORTED BY (name) INTO 2 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
				HiveUtility.getCreateQuery(conf3, writable, partitionBy));

		// This is case of dynamicPartition when static and dynamic both
		// partitions are defined
		Configuration conf4 = new Configuration();
		conf4.set(HIHOConf.HIVE_TABLE_NAME, "employee");
		conf4.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf4.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		conf4.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		conf4.set(HIHOConf.HIVE_CLUSTERED_BY, "name:2");
		conf4.set(HIHOConf.HIVE_SORTED_BY, "name");
		conf4.set(HIHOConf.HIVE_PARTITION_BY, "name:string:raj;country:string");
		// String partitionBy = "country:string";
		String partitionBy1 = "name:string:raj;country:string";
		System.out.println(HiveUtility.getCreateQuery(conf4, writable,
				partitionBy1));
		assertEquals(
				"CREATE TABLE `employee` ( `id` int) PARTITIONED BY (name string,country string) CLUSTERED BY (name) SORTED BY (name) INTO 2 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
				HiveUtility.getCreateQuery(conf4, writable, partitionBy1));

		// This case will throw HIHOException as with clusteredBy conf number of
		// buckets is not defined,it must be defined as name:2
		// please make a note that this case is true for both getCreateQuery()
		// function
		Configuration conf2 = new Configuration();
		conf2.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf2.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		conf2.set(HIHOConf.HIVE_CLUSTERED_BY, "name");
		conf2.set(HIHOConf.HIVE_TABLE_NAME, "employee");
		HiveUtility.getCreateQuery(conf2, writable);

	}

	@Test
	public void testGetcolumns() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "id");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR, "country");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn); // HiveUtility.// = "employee";
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		assertEquals("`id` int,`country` string",
				HiveUtility.getColumns(writable));
	}

	@Test
	public void testGetDynamicPartitionBy() {
		String partitionBy = "name:string:raj;country:string";
		String partitionBy1 = "name:string";
		assertEquals("country", HiveUtility.getDynamicPartitionBy(partitionBy));
		assertEquals("name", HiveUtility.getDynamicPartitionBy(partitionBy1));
	}

	@Test
	public void testGetLoadQuery() throws HIHOException {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		// HiveUtility.tableName = "employee";
		GenericDBWritable writable = new GenericDBWritable(columns, null);
		Configuration config = new Configuration();
		// String partitionBy = "country:string";
		String partitionBy1 = "country:string:us";
		config.set(HIHOConf.INPUT_OUTPUT_PATH, "/user/nube/tableForHiho");
		config.set(HIHOConf.HIVE_TABLE_NAME, "employee");
		config.set(HIHOConf.HIVE_PARTITION_BY, "country:string:us");
		assertEquals(
				"LOAD DATA INPATH '/user/nube/tableForHiho' OVERWRITE INTO TABLE `employee` PARTITION ( country='us')",
				HiveUtility.getLoadQuery(config,
						config.get(HIHOConf.INPUT_OUTPUT_PATH), writable,
						partitionBy1));

		Configuration config1 = new Configuration();
		String partitionBy = "country:string";
		// String partitionBy1 = "country:string:us";
		config1.set(HIHOConf.INPUT_OUTPUT_PATH, "/user/nube/tableForHiho");
		config1.set(HIHOConf.HIVE_TABLE_NAME, "employee");
		// config1.set(HIHOConf.HIVE_PARTITION_BY, "country:string:us");
		assertEquals(
				"LOAD DATA INPATH '/user/nube/tableForHiho' OVERWRITE INTO TABLE `employee`",
				HiveUtility.getLoadQuery(config1,
						config.get(HIHOConf.INPUT_OUTPUT_PATH), writable));

	}

	public void testGetColumnType() throws HIHOException {
		assertEquals("int", HiveUtility.getColumnType(Types.INTEGER));
		assertEquals("long", HiveUtility.getColumnType(Types.BIGINT));
		assertEquals("float", HiveUtility.getColumnType(Types.FLOAT));
		assertEquals("double", HiveUtility.getColumnType(Types.DOUBLE));
		assertEquals("string", HiveUtility.getColumnType(Types.CHAR));
		assertEquals("bytearray", HiveUtility.getColumnType(Types.BINARY));
		assertEquals("bytearray", HiveUtility.getColumnType(Types.BLOB));
		assertEquals("bytearray", HiveUtility.getColumnType(Types.CLOB));

	}
}
