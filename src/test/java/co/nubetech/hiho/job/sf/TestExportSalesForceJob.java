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
package co.nubetech.hiho.job.sf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;

public class TestExportSalesForceJob {

	@Test
	public void testPopulateConfiguration() {
		String[] args = new String[] { "-inputPath", "input", "-sfUserName",
				"sfaccount@hotmail.com", "-sfPassword",
				"tryc,cl,cg123avIotXX9dBlGy3iNiGytlrwy", "-sfObjectType",
				"Account", "-sfHeaders",
				"AccountNumber,Name,BillingState,Phone" };
		ExportSalesForceJob exportSalesForceJob = new ExportSalesForceJob();
		Configuration conf = new Configuration();
		exportSalesForceJob.populateConfiguration(args, conf);

		assertEquals("sfaccount@hotmail.com",
				conf.get(HIHOConf.SALESFORCE_USERNAME));
		assertEquals("tryc,cl,cg123avIotXX9dBlGy3iNiGytlrwy",
				conf.get(HIHOConf.SALESFORCE_PASSWORD));
		assertEquals("Account", conf.get(HIHOConf.SALESFORCE_SOBJECTYPE));
		assertEquals("AccountNumber,Name,BillingState,Phone",
				conf.get(HIHOConf.SALESFORCE_HEADERS));
	}

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-sfUserName",
				"sfaccount@hotmail.com", "-sfPassword",
				"tryc,cl,cg123avIotXX9dBlGy3iNiGytlrwy", "-sfObjectType",
				"Account", "-sfHeaders",
				"AccountNumber,Name,BillingState,Phone" };
		ExportSalesForceJob exportSalesForceJob = new ExportSalesForceJob();
		Configuration conf = new Configuration();
		exportSalesForceJob.populateConfiguration(args, conf);
		exportSalesForceJob.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputPath() throws HIHOException {
		String[] args = new String[] { "-inputPath", "-sfUserName",
				"sfaccount@hotmail.com", "-sfPassword",
				"tryc,cl,cg123avIotXX9dBlGy3iNiGytlrwy", "-sfObjectType",
				"Account", "-sfHeaders",
				"AccountNumber,Name,BillingState,Phone" };
		ExportSalesForceJob exportSalesForceJob = new ExportSalesForceJob();
		Configuration conf = new Configuration();
		exportSalesForceJob.populateConfiguration(args, conf);
		exportSalesForceJob.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForSfUserName() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-sfUserName",
				"-sfPassword", "tryc,cl,cg123avIotXX9dBlGy3iNiGytlrwy",
				"-sfObjectType", "Account", "-sfHeaders",
				"AccountNumber,Name,BillingState,Phone" };
		ExportSalesForceJob exportSalesForceJob = new ExportSalesForceJob();
		Configuration conf = new Configuration();
		exportSalesForceJob.populateConfiguration(args, conf);
		exportSalesForceJob.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForSfPassword() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-sfUserName",
				"username", "-sfPassword", "-sfObjectType", "Account",
				"-sfHeaders", "AccountNumber,Name,BillingState,Phone" };
		ExportSalesForceJob exportSalesForceJob = new ExportSalesForceJob();
		Configuration conf = new Configuration();
		exportSalesForceJob.populateConfiguration(args, conf);
		exportSalesForceJob.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForSfObjectType() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-sfUserName",
				"username", "-sfPassword", "asdf12313", "-sfObjectType",
				"-sfHeaders", "AccountNumber,Name,BillingState,Phone" };
		ExportSalesForceJob exportSalesForceJob = new ExportSalesForceJob();
		Configuration conf = new Configuration();
		exportSalesForceJob.populateConfiguration(args, conf);
		exportSalesForceJob.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForSfHeaders() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-sfUserName",
				"username", "-sfPassword", "asdf12313", "-sfObjectType",
				"Account", "-sfHeaders" };
		ExportSalesForceJob exportSalesForceJob = new ExportSalesForceJob();
		Configuration conf = new Configuration();
		exportSalesForceJob.populateConfiguration(args, conf);
		exportSalesForceJob.checkMandatoryConfs(conf);
	}
}
