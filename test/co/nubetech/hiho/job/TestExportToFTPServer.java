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

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;

public class TestExportToFTPServer {

	@Test
	public void testPopulateConfiguration() {
		String[] args = new String[] { "-inputPath", "input", "-outputPath",
				"output", "-ftpUserName", "sgoyal", "-ftpAddress",
				"192.168.128.3", "-ftpPortNumper", "21", "-ftpPassword",
				"sonalgoyal123" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();

		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);

		assertEquals("sgoyal", conf.get(HIHOConf.FTP_USER));
		assertEquals("192.168.128.3", conf.get(HIHOConf.FTP_ADDRESS));
		assertEquals("21", conf.get(HIHOConf.FTP_PORT));
		assertEquals("sonalgoyal123", conf.get(HIHOConf.FTP_PASSWORD));
	}

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-outputPath",
				"output", "-ftpUserName", "sgoyal", "-ftpAddress",
				"192.168.128.3", "-ftpPortNumper", "21", "-ftpPassword",
				"sonalgoyal123" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();
		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);
		exportToFTPServer.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForInputPath() throws HIHOException {
		String[] args = new String[] { "-inputPath", "-outputPath", "output",
				"-ftpUserName", "sgoyal", "-ftpAddress", "192.168.128.3",
				"-ftpPortNumper", "21", "-ftpPassword", "sonalgoyal123" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();
		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);
		exportToFTPServer.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForOutputPath() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-outputPath",
				"-ftpUserName", "sgoyal", "-ftpAddress", "192.168.128.3",
				"-ftpPortNumper", "21", "-ftpPassword", "sonalgoyal123" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();
		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);
		exportToFTPServer.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForFtpUserName() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-outputPath",
				"output", "-ftpUserName", "-ftpAddress", "192.168.128.3",
				"-ftpPortNumper", "21", "-ftpPassword", "sonalgoyal123" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();
		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);
		exportToFTPServer.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForFtpAddress() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-outputPath",
				"output", "-ftpUserName", "username", "-ftpAddress",
				"-ftpPortNumper", "21", "-ftpPassword", "sonalgoyal123" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();
		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);
		exportToFTPServer.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForFtpPortNumber() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-outputPath",
				"output", "-ftpUserName", "username", "-ftpAddress", "address",
				"-ftpPortNumper", "-ftpPassword", "sonalgoyal123" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();
		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);
		exportToFTPServer.checkMandatoryConfs(conf);
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForFtpPassword() throws HIHOException {
		String[] args = new String[] { "-inputPath", "input", "-outputPath",
				"output", "-ftpUserName", "username", "-ftpAddress", "address",
				"-ftpPortNumper", "21", "-ftpPassword" };
		ExportToFTPServer exportToFTPServer = new ExportToFTPServer();
		Configuration conf = new Configuration();
		exportToFTPServer.populateConfiguration(args, conf);
		exportToFTPServer.checkMandatoryConfs(conf);
	}

}
