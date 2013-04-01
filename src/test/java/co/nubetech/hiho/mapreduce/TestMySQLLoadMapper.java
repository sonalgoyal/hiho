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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;

public class TestMySQLLoadMapper {

	@Test
	public final void testSetup() throws Exception {

		Mapper.Context context = mock(Mapper.Context.class);
		MySQLLoadDataMapper mapper = new MySQLLoadDataMapper();
		/* DriverManager driverManager = mock(DriverManager.class); */
		Configuration conf = new Configuration();
		String url = "jdbc:mysql://localhost:3306/hiho";
		String usrname = "root";
		String password = "newpwd";
		conf.set(DBConfiguration.URL_PROPERTY, url);
		conf.set(DBConfiguration.USERNAME_PROPERTY, usrname);
		conf.set(DBConfiguration.PASSWORD_PROPERTY, password);
		when(context.getConfiguration()).thenReturn(conf);
		mapper.setup(context);
		verify(context, times(3)).getConfiguration();

	}

	@Test
	public final void testMapper() throws Exception {

		Mapper.Context context = mock(Mapper.Context.class);
		MySQLLoadDataMapper mapper = new MySQLLoadDataMapper();
		FSDataInputStream val = mock(FSDataInputStream.class);
		Connection con = mock(Connection.class);
		com.mysql.jdbc.Statement stmt = mock(com.mysql.jdbc.Statement.class);
		mapper.setConnection(con);
		when(
				 con.createStatement(
						ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_UPDATABLE)).thenReturn(stmt);
		Configuration conf = new Configuration();
		conf.set(HIHOConf.LOAD_QUERY_SUFFIX, "abc");
		when(context.getConfiguration()).thenReturn(conf);
		mapper.map(null, val, context);
		verify(stmt).setLocalInfileInputStream(val);

		String query = "load data local infile 'abc.txt' into table abc";
		verify(stmt).executeUpdate(query);

	}
}
