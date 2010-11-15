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

import java.io.*;
import java.net.*;
import org.apache.commons.net.ftp.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.mapreduce.MySQLLoadDataMapper;
import co.nubetech.hiho.mapreduce.OracleLoadMapper;
import co.nubetech.hiho.mapreduce.lib.input.FileStreamInputFormat;

public class OracleLoadMapper extends
		Mapper<NullWritable, FSDataInputStream, NullWritable, NullWritable> {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.OracleLoadMapper.class);

	public void map(NullWritable key, FSDataInputStream val, Context context)
			throws IOException {
		int s = 0;
		// DataInputStream din=val;

		String filename = context.getConfiguration().get(
				HIHOConf.EXTERNAL_TABLE_FILENAME);
		logger.debug("Getting key and value "
				+ HIHOConf.EXTERNAL_TABLE_FILENAME + " : " + filename);
		String ip = context.getConfiguration().get(HIHOConf.ORACLE_FTP_ADDRESS);
		String portno = context.getConfiguration().get(HIHOConf.ORACLE_FTP_PORT);
		String usr = context.getConfiguration().get(HIHOConf.ORACLE_FTP_USER);
		String pwd = context.getConfiguration().get(HIHOConf.ORACLE_FTP_PASSWORD);
		String dir = context.getConfiguration().get(HIHOConf.ORACLE_EXTERNAL_TABLE_DIR);
		logger.debug("Filename is " + filename);
		logger.debug("ip is " + ip);
		logger.debug("port, usr, password are " + portno + ", " + usr + ", "
				+ pwd + "," + dir);
		FTPClient f = new FTPClient();
		f.connect(ip, Integer.parseInt(portno));
		logger.debug("Found connection");
		f.login(usr, pwd);
		logger.debug("Logged in to remote server");
		f.changeWorkingDirectory(dir);
		f.setFileType(FTP.BINARY_FILE_TYPE);
		f.appendFile(filename, val);
		logger.debug("Appened to file");
		val.close();
		f.logout();
		logger.debug("logout from ftp connection");
	}

}
