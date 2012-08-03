# HIHO: Hadoop In, Hadoop Out. 

> Hadoop Data Integration, deduplication, incremental update and more.  

This branch is for support for HIHO on Apache Hadoop 0.21.

## Import from a database to HDFS

**query based import**  

Join multiple tables, provide where conditions, dynamically bind parameters to SQL queries to get data to Hadoop. As simple as creating a simple config and running the job.

	bin/hadoop jar hiho.jar co.nubetech.hiho.job.DBQueryInputJob -conf dbInputQueryDelimited.xml

or
 
	${HIHO_HOME}/scripts/hiho import 
		-jdbcDriver <jdbcDriver> 
		-jdbcUrl <jdbcUrl> 
		-jdbcUsername <jdbcUsername> 
		-jdbcPassword <jdbcPassword> 
		-inputQuery <inputQuery> 
		-inputBoundingQuery <inputBoundingQuery> 
		-outputPath <outputPath> 
		-outputStrategy <outputStrategy> 
		-delimiter <delimiter> 
		-numberOfMappers <numberOfMappers> 
		-inputOrderBy <inputOrderBy> 
 
**table based import**  

	bin/hadoop jar hiho.jar co.nubetech.hiho.job.DBQueryInputJob -conf dbInputTableDelimited.xml

or 

 	${HIHO_HOME}/scripts/hiho import 
		-jdbcDriver <jdbcDriver> 
		-jdbcUrl <jdbcUrl> 
		-jdbcUsername <jdbcUsername> 
		-jdbcPassword <jdbcPassword>
		-outputPath <outputPath> 
		-outputStrategy <outputStrategy> 
		-delimiter <delimiter> 
		-numberOfMappers <numberOfMappers> 
		-inputOrderBy <inputOrderBy> 
		-inputTableName <inputTableName> 
		-inputFieldNames <inputFieldNames>

**incremental import** by appending to existing `HDFS` location so that all data is in one place.
just specify `isAppend = true` in the configurations and import. Import will be written to existing HDFS folder.

**configurable format for data import: delimited, avro** by specifying the `mapreduce.jdbc.hiho.input.outputStrategy` as DELIMITED or AVRO.
 
**Note:** 

1. Please specify delimiter in double qoutes, because in some cases such as semi colon ';' it breaks For example `-delimiter ";"`. If you are specifing * for `inputFieldNames` then also you put in double qoutes  

## Export to Databases

**high performance MySQL loading using LOAD DATA INFILE**

	${HIHO_HOME}/scripts/hiho export mysql
		-inputPath <inputPath> 
		-url <url> 
		-userName <userName> 
		-password <password> 
		-querySuffix  <querySuffix>

**high performance Oracle loading by creating external tables.** See [expert opinion](http://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:6611962171229)  
For information on external tables, check [here](http://download.oracle.com/docs/cd/B12037_01/server.101/b10825/et_concepts.htm)  
On the Oracle server

1. Make a folder

		mkdir -p ageTest

2. Create a directory through the Oracle Client (sqlplus) and grant it privileges.

		sqlplus>create or replace directory age_ext as '/home/nube/age';

3. Allow ftp to the Oracle server
	
		${HIHO_HOME}/scripts/hiho export oracle 
			-inputPath <inputPath> 
			-oracleFtpAddress <oracleFtpAddress> 
			-oracleFtpPortNumber <oracleFtpPortNumber> 
			-oracleFtpUserName <oracleFtpUserName> 
			-oracleFtpPassword <oracleFtpPassword> 
			-oracleExternalTableDirectory <oracleExternalTableDirectory> 
			-driver <driver> 
			-url <url> 
			-userName <userName> 
			-password <password> 
			-externalTable <createExternalTableQuery>

**custom loading and export to any database** by emitting own `GenericDBWritables`. Check `DelimitedLoadMapper`

## Export to SalesForce  

**send computed map reduce results to Salesforce.**

For this, you need to have a developer account with Bulk API enabled. You can join at http://developer.force.com/join
 
If you get message: 

>[LoginFault [ApiFault  exceptionCode='INVALID_LOGIN' exceptionMessage='Invalid username, password, security token; or user locked out.'
"Invalid username, password, security token; or user locked out. Are you at a new location? When accessing Salesforce--either via a desktop client or the API--from outside of your company’s trusted networks, you must add a security token to your password to log in. To receive a new security token, log in to [Salesforce](http://www.salesforce.com) and click Setup | My Personal Information | Reset Security Token."
login and get the security token. 

then try

	sfUserName - Name of Salesforce account
	sfPassword - Password and security token. The Security Token can be obtained by logging in to the Salesforce.com site and clicking on Reset Security Token.
	sfObjectType - The Salesforce object to export
	sfHeaders - header describing the Salesforce object properties. For more information, refer to the Bulk API Developer's Guide.

	${HIHO_HOME}/scripts/hiho export saleforce 
		-inputPath <inputPath> 
		-sfUserName <sfUserName> 
		-sfPassword <sfPassword> 
		-sfObjectType <sfObjectType> 
		-sfHeaders <sfHeaders>


## Export results to an FTP Server.

Use the `co.nubetech.hiho.mapreduce.lib.output.FTPOutputFormat` directly in your job, just like `FileOutputFormat`. For usage, check `co.nubetech.hiho.job.ExportToFTPserver`. This job writes the output directly to an FTP server.
 It can be invoked as:

	${HIHO_HOME}/scripts/hiho export ftp 
		-inputPath <inputPath> 
		-outputPath <outputPath> 
		-ftpUserName <ftpUserName> 
		-ftpAddress <ftpAddress> 
		-ftpPortNumper <ftpPortNumper> 
		-ftpPassword <ftpPassword>

Where:

	ftpUserName - FTP server login username
	ftpAddress - FTP server address
	ftpPortNumper - FTP port
	ftpPassword - FTP server password
	outputPath is the location on the FTP server to which the output will be written. It should be a complete directory path - /home/sgoyal/output  


## Export to Hive
This is used to export data from any other database to Hive database. 
Hive export can be done in two method query base and table based configuration needed are
 
	mapreduce.jdbc.hiho.input.loadTo - this configuration defines in which database you want to load your data from HDFS for eg:- hive
	mapreduce.jdbc.hiho.input.loadToPath - Our program also generates script for all queries , this configuration defines where to store that script on your local system  
	mapreduce.jdbc.hiho.hive.driver - name of hive jdbc driver For eg:- org.apache.hadoop.hive.jdbc.HiveDriver
	mapreduce.jdbc.hiho.hive.url - hive url for jdbc connection For eg:- jdbc:hive:// (for embedded mode),jdbc:hive://localhost:10000/default for standalone mode
	mapreduce.jdbc.hiho.hive.usrName - user name for jdbc connection 
	mapreduce.jdbc.hiho.hive.password - password for jdbc connection
	mapreduce.jdbc.hiho.hive.partitionBy - This configuration is when we want to create partitioned hive table. For eg :- country:string:us;name:string:jack (basic partition), country:string:us;name:string (static and one dynamic partition), country:string (dynamic partition) till now we allow only one dynamic partition
											We also allow to store data in a table for multiple partition at a time for that value is given as country:string:us,uk,aus for this we need to define three different queries or table in there respective configurations 
	mapreduce.jdbc.hiho.hive.ifNotExists - set true if you want include 'if not exits' clause in your create table query
	mapreduce.jdbc.hiho.hive.tableName - write the name for the table in the hive you want to create
	mapreduce.jdbc.hiho.hive.sortedBy - this can be only used if clusteredBy configuration is defined, in this give the name of column by which u want to sort your data
	mapreduce.jdbc.hiho.hive.clusteredBy - This configuration defines name of column by which you want to cluster your data and define the number of buckets you want to create. For eg:- name:2
 
Execution command for table based

 	bin/hadoop jar ~/workspace/hiho/build/classes/hiho.jar co.nubetech.hiho.job.DBQueryInputJob -conf  ~/workspace/hiho/conf/dbInputTableDelimitedHive.xml

or

	${HIHO_HOME}/scripts/hiho import 
		-jdbcDriver <jdbcDriver> 
		-jdbcUrl <jdbcUrl> 
		-jdbcUsername <jdbcUsername> 
		-jdbcPassword <jdbcPassword> 
		-outputPath <outputPath> 
		-outputStrategy <outputStrategy> 
		-delimiter <delimiter> 
		-numberOfMappers <numberOfMappers> 
		-inputOrderBy <inputOrderBy> 
		-inputTableName <inputTableName> 
		-inputFieldNames <inputFieldNames> 
		-inputLoadTo hive 
		-inputLoadToPath <inputLoadToPath> 
		-hiveDriver <hiveDriver>  
		-hiveUrl <hiveUrl> 
		-hiveUsername <hiveUsername> 
		-hivePassword <hivePassword> 
		-hivePartitionBy <hivePartitionBy> 
		-hiveIfNotExists <hiveIfNotExists> 
		-hiveTableName <hiveTableName> 
		-hiveSortedBy <hiveSortedBy> 
		-hiveClusteredBy <hiveClusteredBy> 
 
For query based

	bin/hadoop jar ~/workspace/hiho/build/classes/hiho.jar co.nubetech.hiho.job.DBQueryInputJob -conf  ~/workspace/hiho/conf/dbInputQueryDelimitedHive.xml
or

	${HIHO_HOME}/scripts/hiho import 
		-jdbcDriver <jdbcDriver> 
		-jdbcUrl <jdbcUrl> 
		-jdbcUsername <jdbcUsername> 
		-jdbcPassword <jdbcPassword> 
		-outputPath <outputPath> 
		-outputStrategy <outputStrategy> 
		-delimiter <delimiter> 
		-numberOfMappers <numberOfMappers> 
		-inputOrderBy <inputOrderBy> 
		-inputLoadTo hive 
		-inputLoadToPath <inputLoadToPath> 
		-hiveDriver <hiveDriver>  
		-hiveUrl <hiveUrl> 
		-hiveUsername <hiveUsername> 
		-hivePassword <hivePassword> 
		-hivePartitionBy <hivePartitionBy> 
		-hiveIfNotExists <hiveIfNotExists> 
		-hiveTableName <hiveTableName> 
		-hiveSortedBy <hiveSortedBy> 
		-hiveClusteredBy <hiveClusteredBy>

**Notes:**  

1. Hive table name is mandatory when you are quering more than one query or table that is the case of multiple partition
2. Please note that sorted feature will not work untill clustered feature is defined


## Dedup details

	bin/hadoop jar ~/workspace/HIHO/deploy/hiho.jar co.nubetech.hiho.dedup.DedupJob -inputFormat <inputFormat> -dedupBy <"key" or "value"> -inputKeyClassName <inputKeyClassName> -inputValueClassName <inputValueClassName> -inputPath <inputPath> -outputPath <outputPath> -delimeter <delimeter> -column <column> -outputFormat <outputFormat>

Alternatively Dedup can also be executed as:-
Running HadoopTransform script present in `$HIHO_HOME/scripts/`

	${HIHO_HOME}/scripts/hiho dedup 
		-inputFormat <inputFormat> 
		-dedupBy <"key" or "value"> 
		-inputKeyClassName <inputKeyClassName> 
		-inputValueClassName <inputValueClassName> 
		-inputPath <inputPath> 
		-outputPath <outputPath> 
		-delimeter <delimeter> -column <column>

**Example For Deduplication with key:**  

For Sequence Files:
 
	${HIHO_HOME}/scripts/hiho dedup 
		-inputFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat 
		-inputKeyClassName org.apache.hadoop.io.IntWritable 
		-inputValueClassName org.apache.hadoop.io.Text 
		-inputPath testData/dedup/inputForSeqTest 
		-outputPath output -dedupBy key

For Delimited Text Files: 

	${HIHO_HOME}/scripts/hiho dedup 
		-inputFormat co.nubetech.hiho.dedup.DelimitedTextInputFormat 
		-inputKeyClassName org.apache.hadoop.io.Text 
		-inputValueClassName org.apache.hadoop.io.Text 
		-inputPath testData/dedup/textFilesForTest 
		-outputPath output -delimeter , 
		-column 1 
		-dedupBy key

**Example For Deduplication with value:**  

For Sequence Files: 

	${HIHO_HOME}/scripts/hiho dedup 
		-inputFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat 
		-inputKeyClassName org.apache.hadoop.io.IntWritable 
		-inputValueClassName org.apache.hadoop.io.Text 
		-inputPath testData/dedup/inputForSeqTest 
		-outputPath output -dedupBy value

For Delimited Text Files: 

	${HIHO_HOME}/scripts/hiho dedup 
		-inputFormat co.nubetech.hiho.dedup.DelimitedTextInputFormat 
		-inputKeyClassName org.apache.hadoop.io.Text 
		-inputValueClassName org.apache.hadoop.io.Text 
		-inputPath testData/dedup/textFilesForTest 
		-outputPath output 
		-dedupBy value

## Merge details: 

	${HIHO_HOME}/scripts/hiho merge 
		-newPath <newPath> 
		-oldPath <oldPath> 
		-mergeBy <"key" or "value"> 
		-outputPath <outputPath> 
		-inputFormat <inputFormat> 
		-inputKeyClassName <inputKeyClassName> 
		-inputValueClassName <inputValueClassName> 
		-outputFormat <outputFormat>

**Example For Merge with key:**  

For Sequence Files:

	${HIHO_HOME}/scripts/hiho merge 
		-newPath testData/merge/inputNew/input1.seq 
		-oldPath testData/merge/inputOld/input2.seq 
		-mergeBy key -outputPath output  
		-inputFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat 
		-inputKeyClassName org.apache.hadoop.io.IntWritable 
		-inputValueClassName org.apache.hadoop.io.Text

For Delimited Text Files:

	${HIHO_HOME}/scripts/hiho merge 
		-newPath testData/merge/inputNew/fileInNewPath.txt 
		-oldPath testData/merge/inputOld/fileInOldPath.txt 
		-mergeBy key 
		-outputPath output 
		-inputFormat co.nubetech.hiho.dedup.DelimitedTextInputFormat 
		-inputKeyClassName org.apache.hadoop.io.Text 
		-inputValueClassName org.apache.hadoop.io.Text

**Example For Merge with value:**  

For Sequence Files:

	${HIHO_HOME}/scripts/hiho merge 
		-newPath testData/merge/inputNew/input1.seq 
		-oldPath testData/merge/inputOld/input2.seq 
		-mergeBy value 
		-outputPath output  
		-inputFormat org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat 
		-inputKeyClassName org.apache.hadoop.io.IntWritable 
		-inputValueClassName org.apache.hadoop.io.Text

For Delimited Text Files:

	${HIHO_HOME}/scripts/hiho merge 
		-newPath testData/merge/inputNew/fileInNewPath.txt 
		-oldPath testData/merge/inputOld/fileInOldPath.txt 
		-mergeBy value 
		-outputPath output 
		-inputFormat co.nubetech.hiho.dedup.DelimitedTextInputFormat 
		-inputKeyClassName org.apache.hadoop.io.Text 
		-inputValueClassName org.apache.hadoop.io.Text

## Export to DB:

	bin/hadoop jar deploy/hiho-0.4.0.jar co.nubetech.hiho.job.ExportToDB  
		-jdbcDriver <jdbcDriverName>  
		-jdbcUrl <jdbcUrl>  
		-jdbcUsername <jdbcUserName>  
		-jdbcPassword <jdbcPassword> 
		-delimiter <delimiter> 
		-numberOfMappers <numberOfMappers> 
		-tableName <tableName> 
		-columnNames <columnNames> 
		-inputPath <inputPath> 
or

	${HIHO_HOME}/scripts/hiho export db 
		-jdbcDriver <jdbcDriverName>  
		-jdbcUrl <jdbcUrl>  
		-jdbcUsername <jdbcUserName>  
		-jdbcPassword <jdbcPassword> 
		-delimiter <delimiter> 
		-numberOfMappers <numberOfMappers> 
		-tableName <tableName> 
		-columnNames <columnNames> 
		-inputPath <inputPath> 

## New Features in this release
- incremental import and introduction of AppendFileInputFormat
- Oracle export
- FTP Server integration
- Salesforce
- Support for Apache Hadoop 0.20
- Support for Apache Hadoop 0.21
- Generic dedup and merge

## Other improvements
- Ivy based build and dependency management
- Junit and mockito based test cases 

**Note:** 

1. To run `TestExportToMySQLDB` we need to add `hiho-0.4.0.jar`, all jars of hadoop and hadoop lib with also `mysql-connector-java.jar`
		in the `classpath`.
