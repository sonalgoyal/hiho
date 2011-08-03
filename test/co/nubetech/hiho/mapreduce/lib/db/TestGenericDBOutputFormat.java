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
package co.nubetech.hiho.mapreduce.lib.db;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import co.nubetech.hiho.mapreduce.GenericDBLoadDataMapper;

public class TestGenericDBOutputFormat {
	
	@Test
	public void testPopulateColumnInfo() {
		ArrayList<ColumnInfo> columnInfo = null;

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/hiho", "root", "newpwd");
			PreparedStatement stmt = conn
					.prepareStatement("select * from employee");
			columnInfo = GenericDBWritable.populateColumnInfo(stmt
					.getMetaData());

		} catch (Exception e) {
			e.printStackTrace();
		}
		ArrayList<ColumnInfo> columnInfoActualValue = new ArrayList<ColumnInfo>();
		columnInfoActualValue.add(new ColumnInfo(0, Types.VARCHAR, "name"));
		columnInfoActualValue.add(new ColumnInfo(1, Types.BIGINT, "genderId"));
		columnInfoActualValue.add(new ColumnInfo(2, Types.BIGINT, "dateId"));
		columnInfoActualValue.add(new ColumnInfo(3, Types.VARCHAR,
				"designation"));
		columnInfoActualValue
				.add(new ColumnInfo(4, Types.VARCHAR, "department"));
		columnInfoActualValue
				.add(new ColumnInfo(5, Types.TIMESTAMP, "created"));
		assertEquals(columnInfoActualValue.toString(), columnInfo.toString());

	}
	
	
	@Test
	public final void testSetColumnInfo() throws Exception {
		ArrayList<ColumnInfo> tableInfo = new ArrayList<ColumnInfo>();		
		ColumnInfo columnInfo1 = new ColumnInfo();
		columnInfo1.setIndex(0);
		columnInfo1.setName("id");
		columnInfo1.setType(Types.BIGINT);		
		ColumnInfo columnInfo2 = new ColumnInfo();
		columnInfo2.setIndex(1);
		columnInfo2.setName("name");
		columnInfo2.setType(Types.VARCHAR);	
		tableInfo.add(columnInfo1);
		tableInfo.add(columnInfo2);
		
		String result = GenericDBOutputFormat.getJsonStringOfColumnInfo(tableInfo);
		System.out.print("result: " + result);
		String expectedResult = "[{\"name\":\"id\",\"type\":-5,\"index\":0},{\"name\":\"name\",\"type\":12,\"index\":1}]";
		assertEquals(expectedResult, result);
	}

}
