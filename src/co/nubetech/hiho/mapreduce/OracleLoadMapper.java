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

import java.io.IOException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;

public class OracleLoadMapper extends
		Mapper<Text, FSDataInputStream, NullWritable, NullWritable> {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.OracleLoadMapper.class);

	FTPClient ftpClient;
	public void setFtpClient(FTPClient ftpClient) {
		this.ftpClient=ftpClient;
		
	}
	public FTPClient getFtpClient(){
		return ftpClient;
	}

	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {
		
		String ip = context.getConfiguration().get(HIHOConf.ORACLE_FTP_ADDRESS);
		String portno = context.getConfiguration()
				.get(HIHOConf.ORACLE_FTP_PORT);
		String usr = context.getConfiguration().get(HIHOConf.ORACLE_FTP_USER);
		String pwd = context.getConfiguration().get(
				HIHOConf.ORACLE_FTP_PASSWORD);
		String dir = context.getConfiguration().get(
				HIHOConf.ORACLE_EXTERNAL_TABLE_DIR);
		logger.debug("ip is " + ip);
		logger.debug("port, usr, password are " + portno + ", " + usr + ", "
				+ pwd + "," + dir);
		if (ftpClient==null){
		ftpClient = new FTPClient();
		}
		
		ftpClient.connect(ip, Integer.parseInt(portno));
		logger.debug("Found connection");
		ftpClient.login(usr, pwd);
		logger.debug("Logged in to remote server");
		ftpClient.changeWorkingDirectory(dir);
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
	}

	public void map(Text key, FSDataInputStream val, Context context)
			throws IOException, InterruptedException {
		ftpClient.appendFile(key.toString(), val);
		logger.debug("Appended to file " + key);
		val.close();
	}

	protected void cleanup(Mapper.Context context) throws IOException,
			InterruptedException {
		if (ftpClient != null) {
			ftpClient.disconnect();
			logger.debug("logout from ftp connection");
		}
	}

}
