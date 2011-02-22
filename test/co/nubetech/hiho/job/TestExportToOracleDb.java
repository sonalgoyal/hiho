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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;

public class TestExportToOracleDb {

	@Test
	public void testTableCorrect() throws HIHOException {
		String query = "create table age(  i   Number,  n   Varchar(20),  a   Number)organization external (  type  oracle_loader default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
		assertEquals("age", ExportToOracleDb.getTableName(query));

	}

	@Test
	public void testTableWithSpaceCorrect() throws HIHOException {
		String query = "create table age (  i   Number,  n   Varchar(20),  a   Number)organization external (  type oracle_loader  default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
		assertEquals("age", ExportToOracleDb.getTableName(query));

	}

	@Test(expected = HIHOException.class)
	public void testTableNoBrackets() throws HIHOException {
		String query = "create age ";
		ExportToOracleDb.getTableName(query);

	}

	@Test
	public void testExtDir() throws HIHOException {
		String query = "create table age(  i   Number,  n   Varchar(20),  a   Number)organization external (  type  oracle_loader default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
		assertEquals("ext_dir", ExportToOracleDb.getExternalDir(query));

	}

	@Test
	public void testAlterTableDMl() throws HIHOException, IOException {
		Configuration conf = mock(Configuration.class);
		Path path = mock(Path.class);
		FileStatus status1 = mock(FileStatus.class);
		Path path1 = mock(Path.class);
		when(path1.getName()).thenReturn("part-xxxxx");
		when(status1.getPath()).thenReturn(path1);
		FileStatus status2 = mock(FileStatus.class);
		Path path2 = mock(Path.class);
		when(path2.getName()).thenReturn("part-yyyyy");
		when(status2.getPath()).thenReturn(path2);
		FileSystem fs = mock(FileSystem.class);
		when(fs.listStatus(path)).thenReturn(
				new FileStatus[] { status1, status2 });
		when(path.getFileSystem(conf)).thenReturn(fs);
		when(conf.get(HIHOConf.EXTERNAL_TABLE_DML))
				.thenReturn(
						"create table age(  i   Number,  n   Varchar(20),  a   Number)organization external (  type  oracle_loader default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;");
		String dml = ExportToOracleDb.getAlterTableDML(path, conf);
		assertEquals(" ALTER TABLE age LOCATION ('part-xxxxx','part-yyyyy')",
				dml);
	}

	@Test
	public void testPopulateConfiguration() {
		String[] args = new String[] { "-inputPath", "input",
				"-oracleFtpAddress", "192.168.128.2", "-oracleFtpPortNumber",
				"21", "-oracleFtpUserName", "nube", "-oracleFtpPassword",
				"nube123", "-oracleExternalTableDirectory", "home/nube/age",
				"-driver", "oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();

		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);

		assertEquals("192.168.128.2", conf.get(HIHOConf.ORACLE_FTP_ADDRESS));
		assertEquals("21", conf.get(HIHOConf.ORACLE_FTP_PORT));
		assertEquals("nube", conf.get(HIHOConf.ORACLE_FTP_USER));
		assertEquals("nube123", conf.get(HIHOConf.ORACLE_FTP_PASSWORD));
		assertEquals("home/nube/age",
				conf.get(HIHOConf.ORACLE_EXTERNAL_TABLE_DIR));
		assertEquals("oracle.jdbc.driver.OracleDriver",
				conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
		assertEquals("jdbc:oracle:thin:@192.168.128.2:1521:nube",
				conf.get(DBConfiguration.URL_PROPERTY));
		assertEquals("system", conf.get(DBConfiguration.USERNAME_PROPERTY));
		assertEquals("nube", conf.get(DBConfiguration.PASSWORD_PROPERTY));
	}

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input",
				"-oracleFtpAddress", "192.168.128.2", "-oracleFtpPortNumber",
				"21", "-oracleFtpUserName", "nube", "-oracleFtpPassword",
				"nube123", "-oracleExternalTableDirectory", "home/nube/age",
				"-driver", "oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputPath() throws HIHOException {
		String[] args = new String[] { "-inputPath", "-oracleFtpAddress",
				"192.168.128.2", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "nube", "-oracleFtpPassword", "nube123",
				"-oracleExternalTableDirectory", "home/nube/age", "-driver",
				"oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleFtpAddress()
			throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "nube", "-oracleFtpPassword", "nube123",
				"-oracleExternalTableDirectory", "home/nube/age", "-driver",
				"oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleFtpPortNumber()
			throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber",
				"-oracleFtpUserName", "nube", "-oracleFtpPassword", "nube123",
				"-oracleExternalTableDirectory", "home/nube/age", "-driver",
				"oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleFtpUserName()
			throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "-oracleFtpPassword", "nube123",
				"-oracleExternalTableDirectory", "home/nube/age", "-driver",
				"oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleFtpPassword()
			throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "abc123", "-oracleFtpPassword",
				"-oracleExternalTableDirectory", "home/nube/age", "-driver",
				"oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleExternalTableDirectory()
			throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "abc123", "-oracleFtpPassword",
				"nubeabc", "-oracleExternalTableDirectory", "-driver",
				"oracle.jdbc.driver.OracleDriver", "-url",
				"jdbc:oracle:thin:@192.168.128.2:1521:nube", "-userName",
				"system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleDriver() throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "abc123", "-oracleFtpPassword",
				"nubeabc", "-oracleExternalTableDirectory", "/a/b/c ",
				"-driver", "-url", "jdbc:oracle:thin:@192.168.128.2:1521:nube",
				"-userName", "system", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleUrl() throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "abc123", "-oracleFtpPassword",
				"nubeabc", "-oracleExternalTableDirectory", "/a/b/c ",
				"-driver", "oracle:jdbc:driver", "-url", "-userName", "system",
				"-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOracleUsername() throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "abc123", "-oracleFtpPassword",
				"nubeabc", "-oracleExternalTableDirectory", "/a/b/c ",
				"-driver", "oracle:jdbc:driver", "-url", "jdbc:oracle",
				"-userName", "-password", "nube" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOraclePassword() throws HIHOException {
		String[] args = new String[] { "-inputPath", "inputpath",
				"-oracleFtpAddress", "address", "-oracleFtpPortNumber", "21",
				"-oracleFtpUserName", "abc123", "-oracleFtpPassword",
				"nubeabc", "-oracleExternalTableDirectory", "/a/b/c ",
				"-driver", "oracle:jdbc:driver", "-url", "jdbc:oracle",
				"-userName", "abc", "-password" };
		ExportToOracleDb exportToOracleDb = new ExportToOracleDb();
		Configuration conf = new Configuration();
		exportToOracleDb.populateConfiguration(args, conf);
		exportToOracleDb.checkMandatoryConfs(conf);
	}

}
