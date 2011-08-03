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
package co.nubetech.hiho.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class TestGenericDBLoadDataMapper {
	
	
	@Test
	public final void testMapperWithValidValues() throws Exception {

		Mapper.Context context = mock(Mapper.Context.class);
		GenericDBLoadDataMapper mapper = new GenericDBLoadDataMapper();
		
		mapper.setDelimiter(",");
		ArrayList<ColumnInfo> tableInfo = new ArrayList<ColumnInfo>();
		
		ColumnInfo columnInfo1 = new ColumnInfo();
		columnInfo1.setIndex(0);
		columnInfo1.setName("id");
		columnInfo1.setType(Types.BIGINT);
		
		ColumnInfo columnInfo2 = new ColumnInfo();
		columnInfo2.setIndex(1);
		columnInfo2.setName("name");
		columnInfo2.setType(Types.VARCHAR);
		
		ColumnInfo columnInfo3 = new ColumnInfo();
		columnInfo3.setIndex(2);
		columnInfo3.setName("isValid");
		columnInfo3.setType(Types.BOOLEAN);
		
		/*ColumnInfo columnInfo4 = new ColumnInfo();
		columnInfo4.setIndex(3);
		columnInfo4.setName("date");
		columnInfo4.setType(Types.DATE);*/
		
		ColumnInfo columnInfo5 = new ColumnInfo();
		columnInfo5.setIndex(4);
		columnInfo5.setName("percent");
		columnInfo5.setType(Types.DOUBLE);
		
		tableInfo.add(columnInfo1);
		tableInfo.add(columnInfo2);
		tableInfo.add(columnInfo3);
		//tableInfo.add(columnInfo4);
		tableInfo.add(columnInfo5);
		
		mapper.setTableInfo(tableInfo);
		
		mapper.map(new LongWritable(0l), new Text("1,Sam,true,84.0"), context);
		
		ArrayList values = new ArrayList();
		values.add(1l);
		values.add("Sam");
		values.add(true);
		values.add(84.0);
		GenericDBWritable gdw = new GenericDBWritable(tableInfo, values);
		verify(context).write(gdw, null);

	}
	
	
	@Test
	public final void testMapperWithNullValues() throws Exception {

		Mapper.Context context = mock(Mapper.Context.class);
		GenericDBLoadDataMapper mapper = new GenericDBLoadDataMapper();
		
		mapper.setDelimiter(",");
		ArrayList<ColumnInfo> tableInfo = new ArrayList<ColumnInfo>();
		
		ColumnInfo columnInfo1 = new ColumnInfo();
		columnInfo1.setIndex(0);
		columnInfo1.setName("id");
		columnInfo1.setType(Types.BIGINT);
		
		ColumnInfo columnInfo2 = new ColumnInfo();
		columnInfo2.setIndex(1);
		columnInfo2.setName("name");
		columnInfo2.setType(Types.VARCHAR);
		
		ColumnInfo columnInfo3 = new ColumnInfo();
		columnInfo3.setIndex(2);
		columnInfo3.setName("isValid");
		columnInfo3.setType(Types.BOOLEAN);
		
		/*ColumnInfo columnInfo4 = new ColumnInfo();
		columnInfo4.setIndex(3);
		columnInfo4.setName("date");
		columnInfo4.setType(Types.DATE);*/
		
		ColumnInfo columnInfo5 = new ColumnInfo();
		columnInfo5.setIndex(4);
		columnInfo5.setName("percent");
		columnInfo5.setType(Types.DOUBLE);
		
		tableInfo.add(columnInfo1);
		tableInfo.add(columnInfo2);
		tableInfo.add(columnInfo3);
		//tableInfo.add(columnInfo4);
		tableInfo.add(columnInfo5);
		
		mapper.setTableInfo(tableInfo);
		
		mapper.map(new LongWritable(0l), new Text("1, ,true,84.0"), context);
		
		ArrayList values = new ArrayList();
		values.add(1l);
		values.add(null);
		values.add(true);
		values.add(84.0);
		GenericDBWritable gdw = new GenericDBWritable(tableInfo, values);
		verify(context).write(gdw, null);

	}
	
	@Test(expected=IOException.class)
	public final void testMapperWithUnequalLengthOfColumnInFileAndTable() throws Exception {

		Mapper.Context context = mock(Mapper.Context.class);
		GenericDBLoadDataMapper mapper = new GenericDBLoadDataMapper();
		
		mapper.setDelimiter(",");
		ArrayList<ColumnInfo> tableInfo = new ArrayList<ColumnInfo>();
		
		ColumnInfo columnInfo1 = new ColumnInfo();
		columnInfo1.setIndex(0);
		columnInfo1.setName("id");
		columnInfo1.setType(Types.BIGINT);
		
		ColumnInfo columnInfo2 = new ColumnInfo();
		columnInfo2.setIndex(1);
		columnInfo2.setName("name");
		columnInfo2.setType(Types.VARCHAR);
		
		ColumnInfo columnInfo3 = new ColumnInfo();
		columnInfo3.setIndex(2);
		columnInfo3.setName("isValid");
		columnInfo3.setType(Types.BOOLEAN);
		
		/*ColumnInfo columnInfo4 = new ColumnInfo();
		columnInfo4.setIndex(3);
		columnInfo4.setName("date");
		columnInfo4.setType(Types.DATE);*/
		
		ColumnInfo columnInfo5 = new ColumnInfo();
		columnInfo5.setIndex(4);
		columnInfo5.setName("percent");
		columnInfo5.setType(Types.DOUBLE);
		
		tableInfo.add(columnInfo1);
		tableInfo.add(columnInfo2);
		tableInfo.add(columnInfo3);
		//tableInfo.add(columnInfo4);
		tableInfo.add(columnInfo5);
		
		mapper.setTableInfo(tableInfo);
		
		mapper.map(new LongWritable(0l), new Text("1,Sam,true,84.0,42"), context);
	}
	
	
	@Test
	public final void testSetUp() throws Exception {

		Mapper.Context context = mock(Mapper.Context.class);
		Configuration conf = mock(Configuration.class);
		GenericDBLoadDataMapper mapper = new GenericDBLoadDataMapper();
		
		mapper.setDelimiter(",");
		//Configuration conf = new Configuration();
		when(context.getConfiguration()).thenReturn(conf);
		when(context.getConfiguration().get(HIHOConf.INPUT_OUTPUT_DELIMITER)).thenReturn(",");
		ArrayList<ColumnInfo> tableInfo = new ArrayList<ColumnInfo>();
		
		ColumnInfo columnInfo1 = new ColumnInfo();
		columnInfo1.setIndex(0);
		columnInfo1.setName("id");
		columnInfo1.setType(Types.BIGINT);
		
		ColumnInfo columnInfo2 = new ColumnInfo();
		columnInfo2.setIndex(1);
		columnInfo2.setName("name");
		columnInfo2.setType(Types.VARCHAR);
		
		ColumnInfo columnInfo3 = new ColumnInfo();
		columnInfo3.setIndex(2);
		columnInfo3.setName("isValid");
		columnInfo3.setType(Types.BOOLEAN);
		
		/*ColumnInfo columnInfo4 = new ColumnInfo();
		columnInfo4.setIndex(3);
		columnInfo4.setName("date");
		columnInfo4.setType(Types.DATE);*/
		
		ColumnInfo columnInfo5 = new ColumnInfo();
		columnInfo5.setIndex(4);
		columnInfo5.setName("percent");
		columnInfo5.setType(Types.DOUBLE);
		
		tableInfo.add(columnInfo1);
		tableInfo.add(columnInfo2);
		tableInfo.add(columnInfo3);
		//tableInfo.add(columnInfo4);
		tableInfo.add(columnInfo5);
		
		ObjectMapper objectMapper = new ObjectMapper();			
		JsonFactory jsonFactory = new JsonFactory();	
		StringWriter writer = new StringWriter();
		JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
		objectMapper.writeValue(jsonGenerator, tableInfo);
		String jsonString = writer.toString();
		when(context.getConfiguration().get(HIHOConf.COLUMN_INFO)).thenReturn(jsonString);
		
		mapper.setup(context);
		assertEquals(mapper.getTableInfo().toString(), tableInfo.toString());
		

	}
}
