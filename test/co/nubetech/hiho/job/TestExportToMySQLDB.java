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
import co.nubetech.hiho.common.HIHOException;

public class TestExportToMySQLDB {

	@Test
	public void testPopulateConfiguration() {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();

		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);

		assertEquals("jdbc:mysql://localhost:3306/hiho",
				conf.get(DBConfiguration.URL_PROPERTY));
		assertEquals("root", conf.get(DBConfiguration.USERNAME_PROPERTY));
		assertEquals("newpwd", conf.get(DBConfiguration.PASSWORD_PROPERTY));
	}

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputPath() throws HIHOException {
		String[] args = new String[] { "-inputPath", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "root",
				"-password", "newpwd" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForJdbcUrl() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"-userName", "root", "-password", "newpwd" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForJdbcUserName() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "-password",
				"newpwd" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForJdbcPassword() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-url",
				"jdbc:mysql://localhost:3306/hiho", "-userName", "username",
				"-password" };
		ExportToMySQLDB exportToMySQLDB = new ExportToMySQLDB();
		Configuration conf = new Configuration();
		exportToMySQLDB.populateConfiguration(args, conf);
		exportToMySQLDB.checkMandatoryConfs(conf);
	}

}
