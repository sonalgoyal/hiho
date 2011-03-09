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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.HihoTestCase;

public class TestExportToMySQLDB extends HihoTestCase {

	@Test
	public void testPopulateConfiguration() {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd", "-querySuffix",
				"mrTest fields terminated by ','" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();

		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);

		assertEquals("jdbc:mysql://localhost:3306/hiho",
				conf.get(DBConfiguration.URL_PROPERTY));
		assertEquals("root", conf.get(DBConfiguration.USERNAME_PROPERTY));
		assertEquals("newpwd", conf.get(DBConfiguration.PASSWORD_PROPERTY));
		assertEquals("mrTest fields terminated by ','",
				conf.get(HIHOConf.LOAD_QUERY_SUFFIX));
	}

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd", "-querySuffix",
				"mrTest fields terminated by ','" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputPath() throws HIHOException {
		String[] args = new String[] { "-inputPath", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd", "-querySuffix",
				"mrTest fields terminated by ','" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForJdbcUrl() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"-userName", "root", "-password", "newpwd", "-querySuffix",
				"mrTest fields terminated by ','" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForJdbcUserName() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "-password",
				"newpwd", "-querySuffix", "mrTest fields terminated by ','" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForJdbcPassword() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "username",
				"-password", "-querySuffix", "mrTest fields terminated by ','" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForQuerySuffix() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "username",
				"-password", "passwd", "-querySuffix" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test
	public void testMySQlBasicExport() throws Exception {

		final String inputData1 = "1,Dale Zamora,manager"
				+ "\n2,Macaulay Jackson,clerk" + "\n3,Charles Wood,supervisior";
		createTextFileInHDFS(inputData1, "/input", "testFile1.txt");

		String[] args = new String[] { "-inputPath", "/input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd", "-querySuffix",
				"employee fields terminated by ',' lines terminated by '\n'" };
		ExportToMySQLDB job = new ExportToMySQLDB();
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/hiho", "root", "newpwd");
		com.mysql.jdbc.Statement stmt = (com.mysql.jdbc.Statement) conn
				.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
		String query = "Select * from employee";
		ResultSet rs = stmt.executeQuery(query);
		String output = "";
		while (rs.next()) {
			output = output + rs.getString(1) + "," + rs.getString(2) + ","
					+ rs.getString(3);
			output = output + "\n";
		}
		assertEquals(inputData1 + "\n", output);
		stmt.executeUpdate("delete from employee");
		rs.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMySQlBasicExportForDelimiter() throws Exception {

		final String inputData1 = "1||Dale Zamora||manager"
				+ "\n2||Macaulay Jackson||clerk"
				+ "\n3||Charles Wood||supervisior";
		createTextFileInHDFS(inputData1, "/input", "testFile1.txt");

		String[] args = new String[] { "-inputPath", "/input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd", "-querySuffix",
				"employee fields terminated by '||' lines terminated by '\n'" };
		ExportToMySQLDB job = new ExportToMySQLDB();
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/hiho", "root", "newpwd");
		com.mysql.jdbc.Statement stmt = (com.mysql.jdbc.Statement) conn
				.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
		String query = "Select * from employee";
		ResultSet rs = stmt.executeQuery(query);
		String output = "";
		while (rs.next()) {
			output = output + rs.getString(1) + "||" + rs.getString(2) + "||"
					+ rs.getString(3);
			output = output + "\n";
		}
		assertEquals(inputData1 + "\n", output);
		stmt.executeUpdate("delete from employee");
		rs.close();
		stmt.close();
		conn.close();
	}
}
