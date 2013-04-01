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
package co.nubetech.hiho.mapreduce;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;

public class TestOracleLoadMapper {

	@Test
	public final void testSetup() throws Exception {
		Mapper.Context context = mock(Mapper.Context.class);
		OracleLoadMapper mapper = new OracleLoadMapper();
		FTPClient ftpClient = mock(FTPClient.class);
		Configuration conf = new Configuration();
		String ip = "192.168.128.8";
		String portno = "21";
		String user = "nube";
		String password = "nube123";
		String externalDirectory = "dir";
		conf.set(HIHOConf.ORACLE_FTP_ADDRESS, ip);
		conf.set(HIHOConf.ORACLE_FTP_PORT, portno);
		conf.set(HIHOConf.ORACLE_FTP_USER, user);
		conf.set(HIHOConf.ORACLE_FTP_PASSWORD, password);
		conf.set(HIHOConf.ORACLE_EXTERNAL_TABLE_DIR, externalDirectory);
		when(context.getConfiguration()).thenReturn(conf);
		mapper.setFtpClient(ftpClient);
		mapper.setup(context);
		verify(ftpClient).connect(ip, Integer.parseInt(portno));
		verify(ftpClient).login(user, password);
		verify(ftpClient).changeWorkingDirectory(externalDirectory);
	}

	@Test
	public final void testMapper() throws Exception {
		Mapper.Context context = mock(Mapper.Context.class);
		OracleLoadMapper mapper = new OracleLoadMapper();
		FTPClient ftpClient = mock(FTPClient.class);
		FSDataInputStream val=mock(FSDataInputStream.class);
		Text key = new Text("key");
		mapper.setFtpClient(ftpClient);
		mapper.map(key, val, context);
		verify(ftpClient).appendFile("key", val);
	}

}
