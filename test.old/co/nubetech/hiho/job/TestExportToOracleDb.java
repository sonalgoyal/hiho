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

import static org.junit.Assert.*;

import java.io.IOException;

import junit.framework.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.common.HIHOException;


public class TestExportToOracleDb {
	
	@Test
	public void testTableCorrect() throws HIHOException {
		String query = "create table age(  i   Number,  n   Varchar(20),  a   Number)organization external (  type  oracle_loader default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
	    assertEquals("age", ExportToOracleDb.getTableName(query));
	    
	}
	
	@Test
	public void testTableWithSpaceCorrect() throws HIHOException {
		String query = "create table age (  i   Number,  n   Varchar(20),  a   Number)organization external (  type oracle_loader  default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
	    assertEquals("age", ExportToOracleDb.getTableName(query));
	    
	}
	
	@Test(expected=HIHOException.class)
	public void testTableNoBrackets() throws HIHOException {
		String query = "create age ";
	    ExportToOracleDb.getTableName(query);
	    
	}
	
	@Test
	public void testExtDir() throws HIHOException {
		String query = "create table age(  i   Number,  n   Varchar(20),  a   Number)organization external (  type  oracle_loader default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;";
	    assertEquals("ext_dir", ExportToOracleDb.getExternalDir(query));
	    
	}
	
	@Test
	public void testAlterTableDMl() throws HIHOException, IOException {
		Configuration conf = mock(Configuration.class);
		Path path = mock(Path.class);
		FileStatus status1 = mock(FileStatus.class);
		Path path1 = mock(Path.class);
		when (path1.getName()).thenReturn("part-xxxxx");
		when (status1.getPath()).thenReturn(path1);
		FileStatus status2 = mock(FileStatus.class);
		Path path2 = mock(Path.class);
		when (path2.getName()).thenReturn("part-yyyyy");
		when (status2.getPath()).thenReturn(path2);
		FileSystem fs = mock(FileSystem.class);
		when (fs.listStatus(path)).thenReturn(new FileStatus[] {status1, status2});
		when(path.getFileSystem(conf)).thenReturn(fs);
		when(conf.get(HIHOConf.EXTERNAL_TABLE_DML)).thenReturn("create table age(  i   Number,  n   Varchar(20),  a   Number)organization external (  type  oracle_loader default directory ext_dir access parameters (records delimited  by newlinefields  terminated by ','missing field values are null )location  (/home/nube/:file.txt) reject' limit unlimited;");
		String dml =  ExportToOracleDb.getAlterTableDML(path, conf);
		assertEquals(" ALTER TABLE age LOCATION ('part-xxxxx','part-yyyyy')", dml);
	}
}	
	
