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

package co.nubetech.hiho.common;

public interface HIHOConf {
	
	public static final String QUERY_PARAMS = "mapreduce.jdbc.hiho.input.params";
	public static final String INPUT_OUTPUT_STRATEGY = "mapreduce.jdbc.hiho.input.outputStrategy";
	public static final String INPUT_OUTPUT_PATH = "mapreduce.jdbc.hiho.input.outputPath";
	public static final String INPUT_OUTPUT_DELIMITER = "mapreduce.jdbc.hiho.input.delimiter";
	public static final String BEAN_PROPERTY_NAME = "mapreduce.jdbc.hiho.bean.property.name";
	public static final String BEAN_PROPERTY_CONVERTORS="mapreduce.jdbc.hiho.bean.property.convertors";
	public static final String IS_APPEND = "mapreduce.jdbc.hiho.output.append";
	public static final String INPUT_OUTPUT_LOADTO = "mapreduce.jdbc.hiho.input.loadTo";
	public static final String INPUT_OUTPUT_LOADTO_PATH = "mapreduce.jdbc.hiho.input.loadToPath";
	//for loading data to MySQL database
	public static final String LOAD_QUERY_SUFFIX = "mapreduce.jdbc.hiho.load.query.suffix";
	public static final String NUMBER_MAPPERS = "mapreduce.jdbc.hiho.number.mappers";
	
	//for loading oracle data
	public static final String EXTERNAL_TABLE_DML = "mapreduce.jdbc.hiho.oracle.externaltable.dml";
	public static final String EXTERNAL_TABLE_FILENAME = "mapreduce.jdbc.hiho.oracle.externaltable.file";
	public static final String ORACLE_FTP_ADDRESS="mapreduce.jdbc.hiho.oracle.ftp.serveraddress";
	public static final String ORACLE_FTP_PORT="mapreduce.jdbc.hiho.oracle.ftp.portnumber";
	public static final String ORACLE_FTP_USER="mapreduce.jdbc.hiho.oracle.ftp.username";
	public static final String ORACLE_FTP_PASSWORD="mapreduce.jdbc.hiho.oracle.ftp.password";
	public static final String ORACLE_EXTERNAL_TABLE_DIR="mapreduce.jdbc.hiho.oracle.ftp.extdir";
	//public static final String DBINSTANCE="mapreduce.jdbc.hiho.oracle.dbinstance";
	//ip, port, username, password
	
	//for loading saleforce data
	public static final String SALESFORCE_USERNAME="mapreduce.jdbc.hiho.sf.username";
	public static final String SALESFORCE_PASSWORD="mapreduce.jdbc.hiho.sf.password";
	public static final String SALESFORCE_SOBJECTYPE="mapreduce.jdbc.hiho.sf.sobjectype";
	public static final String SALESFORCE_HEADERS="mapreduce.jdbc.hiho.sf.headers";
	
	//for loading data to ftp server
	public static final String FTP_ADDRESS="mapreduce.hiho.ftp.serveraddress";
	public static final String FTP_PORT="mapreduce.hiho.ftp.portnumber";
	public static final String FTP_USER="mapreduce.hiho.ftp.username";
	public static final String FTP_PASSWORD="mapreduce.hiho.ftp.password";	
	//public static final String FTP_DIR="mapreduce.hiho.ftp.dir";
	
	//for loading data in hive
	public static final String HIVE_DRIVER="mapreduce.jdbc.hiho.hive.driver";
	public static final String HIVE_USR_NAME="mapreduce.jdbc.hiho.hive.usrname";
	public static final String HIVE_PASSWORD="mapreduce.jdbc.hiho.hive.password";
	public static final String HIVE_URL="mapreduce.jdbc.hiho.hive.url";
	

}
