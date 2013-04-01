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
package co.nubetech.hiho.job;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;


public class TestDBQueryInputJob {
	@Test
	public void testPopulateConfigurations() throws HIHOException {
		Configuration conf = new Configuration();
		String[] args = new String[] { "-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/", "-jdbcUsername",
				"root", "-jdbcPassword", "root", "-outputPath", "/tmp",
				"-outputStrategy", "DELIMITED", "-delimiter", "|",
				"-numberOfMappers", "2", "-inputTableName", "table",
				"-inputFieldNames", "field1,field2", "-inputOrderBy", "field1",
				"-inputLoadTo", "pig", "-inputLoadToPath", "/tmp",
				"-hiveDriver", "org.apache.hadoop.hive.jdbc.HiveDriver",
				"-hiveUrl", "jdbc:hive://localhost:10000/", "-hiveUsername",
				"hive", "-hivePassword", "hive", "-hivePartitionBy",
				"country:string:india", "-hiveIfNotExists", "true",
				"-hiveTableName", "table", "-hiveSortedBy", "id",
				"-hiveClusteredBy", "country:2","-inputQuery","select * from student",
				"-inputBoundingQuery","select min(id), max(id) from student"};

		new DBQueryInputJob().populateConfiguration(args, conf);
		assertEquals("com.mysql.jdbc.Driver",
				conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
		assertEquals("jdbc:mysql://localhost:3306/",
				conf.get(DBConfiguration.URL_PROPERTY));
		assertEquals("root", conf.get(DBConfiguration.USERNAME_PROPERTY));
		assertEquals("root", conf.get(DBConfiguration.PASSWORD_PROPERTY));
		assertEquals("/tmp", conf.get(HIHOConf.INPUT_OUTPUT_PATH));
		assertEquals("DELIMITED", conf.get(HIHOConf.INPUT_OUTPUT_STRATEGY));
		assertEquals("|", conf.get(HIHOConf.INPUT_OUTPUT_DELIMITER));
		assertEquals("2", conf.get(HIHOConf.NUMBER_MAPPERS));
		assertEquals("table",
				conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY));
		assertEquals("field1,field2",
				conf.get(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY));
		assertEquals("field1",
				conf.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY));
		assertEquals("pig", conf.get(HIHOConf.INPUT_OUTPUT_LOADTO));
		assertEquals("/tmp", conf.get(HIHOConf.INPUT_OUTPUT_LOADTO_PATH));
		assertEquals("org.apache.hadoop.hive.jdbc.HiveDriver",
				conf.get(HIHOConf.HIVE_DRIVER));
		assertEquals("jdbc:hive://localhost:10000/",
				conf.get(HIHOConf.HIVE_URL));
		assertEquals("hive", conf.get(HIHOConf.HIVE_USR_NAME));
		assertEquals("hive", conf.get(HIHOConf.HIVE_PASSWORD));
		assertEquals("country:string:india",
				conf.get(HIHOConf.HIVE_PARTITION_BY));
		assertEquals("true", conf.get(HIHOConf.HIVE_TABLE_OVERWRITE));
		assertEquals("table", conf.get(HIHOConf.HIVE_TABLE_NAME));
		assertEquals("id", conf.get(HIHOConf.HIVE_SORTED_BY));
		assertEquals("country:2", conf.get(HIHOConf.HIVE_CLUSTERED_BY));
		assertEquals("select * from student", conf.get(DBConfiguration.INPUT_QUERY));
		assertEquals("select min(id), max(id) from student",
				conf.get(DBConfiguration.INPUT_BOUNDING_QUERY));
	}

	@Test
	public void testCheckMandatoryConfs() throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");
		new DBQueryInputJob().checkMandatoryConfs(conf);

	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForDriverClass() throws HIHOException {
		Configuration conf = new Configuration();
		// conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);

	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForUrlProperty() throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		// conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForUsernameProperty()
			throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		// conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForPasswordProperty()
			throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		// conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOutputPath() throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		// conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForDelimiter() throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		// conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_QUERY, "value");
		conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputTableNameAndInputQuery()
			throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		// conf.set(DBConfiguration.INPUT_QUERY, "value");
		// conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForHiveLoadToPath() throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_QUERY, "value");
		conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		// conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForHiveDriver() throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_QUERY, "value");
		conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		// conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");

		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForHiveUrl() throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_QUERY, "value");
		conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		// conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");
		new DBQueryInputJob().checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForHiveOutputTableNameInCaseOfMultiPartition()
			throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_QUERY, "value");
		conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value:us,india");
		// conf.set(HIHOConf.HIVE_TABLE_NAME, "value");
		new DBQueryInputJob().checkMandatoryConfs(conf);
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputBoundingQuery()
			throws HIHOException {
		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "value");
		conf.set(DBConfiguration.URL_PROPERTY, "value");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "value");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_PATH, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_STRATEGY, "DELIMITED");
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, "value");
		conf.set(DBConfiguration.INPUT_QUERY, "value");
	//	conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, "value");
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "value");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO, "hive");
		conf.set(HIHOConf.INPUT_OUTPUT_LOADTO_PATH, "value");
		conf.set(HIHOConf.HIVE_DRIVER, "value");
		conf.set(HIHOConf.HIVE_URL, "value");
		conf.set(HIHOConf.HIVE_PARTITION_BY, "value:value:us,india");
		conf.set(HIHOConf.HIVE_TABLE_NAME, "value");
		new DBQueryInputJob().checkMandatoryConfs(conf);
	}
}
