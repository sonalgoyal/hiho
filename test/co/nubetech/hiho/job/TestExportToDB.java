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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.HihoTestCase;

public class TestExportToDB extends HihoTestCase {
	final static Logger logger = Logger
	.getLogger(co.nubetech.hiho.job.TestExportToDB.class);
	
	@Test
	public void testCheckMandatoryConfsForValidValues() throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForDriverClass() throws HIHOException {
		String[] args = new String[] {
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);

	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForUrlProperty() throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForUsernameProperty()
			throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForPasswordProperty()
			throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsFordelimiter() throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForNumberOfMappers() throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForTableName() throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForColumnNames() throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-inputPath", "/input1,/input2"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}
	
	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputPath() throws HIHOException {
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department"};	
		ExportToDB exportToDB = new ExportToDB();
		Configuration conf = new Configuration();
		exportToDB.populateConfiguration(args, conf);
		exportToDB.checkMandatoryConfs(conf);
	}
	
	@Test
	public void testExportToDBWithValidValues() throws Exception{
		final String inputData1 = "Xavier Wilson,1,99999,ASE,IT\n" + 
		"Drake Mckinney,1,99999,SSE,IT";
		final String inputData2 = "Zephania Bauer,2,99999,PM,IT";
		
		createTextFileInHDFS(inputData1, "/input", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input", "testFile2.txt");	
		
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input"};	
		
		int res = ToolRunner.run(createJobConf(), new ExportToDB(), args);
		assertEquals(0, res);
		
		String userName = "root";
		String password = "newpwd";
		String url = "jdbc:mysql://localhost/hiho";
		Connection conn;
		Statement stmt;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(url, userName, password);
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from employee where name='Xavier Wilson'");
			boolean isRsExists = false;
			while(rs.next()){
				assertEquals(rs.getString("name"),"Xavier Wilson");
				assertEquals(rs.getLong("genderId"),1l);
				assertEquals(rs.getLong("dateId"),99999l);
				assertEquals(rs.getString("designation"),"ASE");
				assertEquals(rs.getString("department"),"IT");
				isRsExists = true;				
			}
			assertTrue(isRsExists);
			stmt.executeUpdate("delete from employee where dateId=99999");			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testExportToDBWithValidValuesNullEntry() throws Exception{
		final String inputData1 = "Xavier Wilson,1,99999, ,IT\n" + 
		"Drake Mckinney,1,99999,ASE ,IT";
		final String inputData2 = "Zephania Bauer,2,99999,PM,IT";
		
		createTextFileInHDFS(inputData1, "/input", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input", "testFile2.txt");	
		
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input"};	
		
		int res = ToolRunner.run(createJobConf(), new ExportToDB(), args);
		assertEquals(0, res);
		
		String userName = "root";
		String password = "newpwd";
		String url = "jdbc:mysql://localhost/hiho";
		Connection conn;
		Statement stmt;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(url, userName, password);
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from employee where name='Xavier Wilson'");
			boolean isRsExists = false;
			while(rs.next()){
				assertEquals(rs.getString("name"),"Xavier Wilson");
				assertEquals(rs.getLong("genderId"),1l);
				assertEquals(rs.getLong("dateId"),99999l);
				assertEquals(rs.getString("designation"),null);
				assertEquals(rs.getString("department"),"IT");
				isRsExists = true;				
			}
			assertTrue(isRsExists);
			stmt.executeUpdate("delete from employee where dateId=99999");			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testExportToDBWithUnequalLengthOfColumns() throws Exception{
		final String inputData1 = "Xavier Wilson,1,99999,ASE,IT\n" + 
		"Drake Mckinney,1,99999,SSE,IT,425";
		final String inputData2 = "Zephania Bauer,2,99999,PM,IT";
		
		createTextFileInHDFS(inputData1, "/input", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input", "testFile2.txt");	
		
		String[] args = new String[] {
				"-jdbcDriver", "com.mysql.jdbc.Driver",
				"-jdbcUrl", "jdbc:mysql://localhost:3306/hiho",
				"-jdbcUsername", "root",
				"-jdbcPassword", "newpwd",
				"-delimiter", ",",
				"-numberOfMappers", "1",
				"-tableName", "employee",
				"-columnNames", "name,genderId,dateId,designation,department",
				"-inputPath", "/input"};	
		
		int res = ToolRunner.run(createJobConf(), new ExportToDB(), args);
		assertEquals(1, res);
	}

}
