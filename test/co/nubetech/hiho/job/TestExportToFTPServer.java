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

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.HihoTestCase;

public class TestExportToFTPServer extends HihoTestCase {

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
	
	//still to complete.
	/*@Test
	public void testFtpJob() throws Exception {
		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory factory = new ListenerFactory();
		// set the port of the listener
		factory.setPort(2221);
		// replace the default listener
		serverFactory.addListener("default", factory.createListener());
				
        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        userManagerFactory.setFile(new File("/tmp/myusers.properties"));
        UserManager um = userManagerFactory.createUserManager();
        
        BaseUser user = new BaseUser();
        user.setName("nube");
        user.setPassword("nube123");
        user.setHomeDirectory("/tmp");
        
        um.save(user);
        FtpServer server = serverFactory.createServer();
		server.start();
		
		final String inputData = "A\tTimon Leonard,716 Ac Ave,1-857-935-3882,62240"
				+ "\nD\tMacaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"
				+ "\nB\tCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		createTextFileInHDFS(inputData, "/input", "testFile.txt");

		String[] args = new String[] { "-inputPath", "/input", "-outputPath",
				"/tmp/output", "-ftpUserName", "nube", "-ftpAddress", "192.168.128.8",
				"-ftpPortNumper", "2221", "-ftpPassword", "nube123" };
		ExportToFTPServer job = new ExportToFTPServer();
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);
		
	}*/
}
