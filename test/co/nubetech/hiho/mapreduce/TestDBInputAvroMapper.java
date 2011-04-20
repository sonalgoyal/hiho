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

import java.io.IOException;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import co.nubetech.hiho.avro.DBMapper;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class TestDBInputAvroMapper {

	private NullWritable nullWritable = NullWritable.get();

	@Test
	public final void testMapperValidValues() throws IOException,
			InterruptedException {
		OutputCollector<AvroValue<Pair>, NullWritable> output = mock(OutputCollector.class);
		Reporter reporter = mock(Reporter.class);

		DBInputAvroMapper mapper = new DBInputAvroMapper();

		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ColumnInfo dateColumn = new ColumnInfo(1, Types.DATE, "dateColumn");
		ColumnInfo longColumn = new ColumnInfo(1, Types.BIGINT, "longColumn");
		ColumnInfo booleanColumn = new ColumnInfo(1, Types.BOOLEAN,
				"booleanColumn");
		ColumnInfo doubleColumn = new ColumnInfo(1, Types.DOUBLE,
				"doubleColumn");
		ColumnInfo floatColumn = new ColumnInfo(1, Types.FLOAT,
		 "floatColumn");
		ColumnInfo charColumn = new ColumnInfo(1, Types.CHAR, "charColumn");
		ColumnInfo timeColumn = new ColumnInfo(1, Types.TIME, "timeColumn");
		ColumnInfo timeStampColumn = new ColumnInfo(1, Types.TIMESTAMP,
				"timeStampColumn");

		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();

		columns.add(intColumn);
		columns.add(stringColumn);
		columns.add(dateColumn);
		columns.add(longColumn);
		columns.add(booleanColumn);
		columns.add(doubleColumn);
		columns.add(floatColumn);
		columns.add(charColumn);
		columns.add(timeColumn);
		columns.add(timeStampColumn);

		ArrayList values = new ArrayList();
		values.add(new Integer(12));
		values.add(new String("sam"));
		values.add(new Date());
		values.add(new Long(26564l));
		values.add(true);
		values.add(1.235);
		values.add(new Float(1.0f));
		values.add('a');
		values.add(new Time(new Date().getTime()));
		values.add(new Time(new Date().getTime()));

		GenericDBWritable val = new GenericDBWritable(columns, values);
		LongWritable key = new LongWritable(1);

		Schema pairSchema = DBMapper.getPairSchema(val.getColumns());
		Schema keySchema = Pair.getKeySchema(pairSchema);
		Schema valueSchema = Pair.getValueSchema(pairSchema);
		Pair pair = new Pair<GenericRecord, GenericRecord>(pairSchema);

		GenericRecord keyRecord = new GenericData.Record(keySchema);
		keyRecord.put(0, key.get());
		GenericRecord valueRecord = new GenericData.Record(valueSchema);
		List<Schema.Field> fieldSchemas = valueSchema.getFields();
		for (int i = 0; i < val.getValues().size(); ++i) {
			Schema.Type type = fieldSchemas.get(i).schema().getType();
			if (type.equals(Schema.Type.STRING)) {
				Utf8 utf8 = new Utf8((String) val.getValues().get(i).toString());
				valueRecord.put(i, utf8);
			} else {
				valueRecord.put(i, val.getValues().get(i));
			}
		}
		pair.key(keyRecord);
		pair.value(valueRecord);

		mapper.map(key, val, output, reporter);
		
		verify(output).collect(new AvroValue<Pair>(pair), nullWritable);
	}

	@Test 
	public void testGetKeyRecord(){
		DBInputAvroMapper mapper = new DBInputAvroMapper();

		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ColumnInfo dateColumn = new ColumnInfo(1, Types.DATE, "dateColumn");
		ColumnInfo longColumn = new ColumnInfo(1, Types.BIGINT, "longColumn");
		ColumnInfo booleanColumn = new ColumnInfo(1, Types.BOOLEAN,
				"booleanColumn");
		ColumnInfo doubleColumn = new ColumnInfo(1, Types.DOUBLE,
				"doubleColumn");
		// ColumnInfo floatColumn = new ColumnInfo(1, Types.FLOAT,
		// "floatColumn");
		ColumnInfo charColumn = new ColumnInfo(1, Types.CHAR, "charColumn");
		ColumnInfo timeColumn = new ColumnInfo(1, Types.TIME, "timeColumn");
		ColumnInfo timeStampColumn = new ColumnInfo(1, Types.TIMESTAMP,
				"timeStampColumn");

		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();

		columns.add(intColumn);
		columns.add(stringColumn);
		columns.add(dateColumn);
		columns.add(longColumn);
		columns.add(booleanColumn);
		columns.add(doubleColumn);
		// columns.add(floatColumn);
		columns.add(charColumn);
		columns.add(timeColumn);
		columns.add(timeStampColumn);

		ArrayList values = new ArrayList();
		values.add(new Integer(12));
		values.add(new String("sam"));
		values.add(new Date());
		values.add(new Long(26564l));
		values.add(true);
		values.add(1.235);
		// values.add(new Float(1.0f));
		values.add('a');
		values.add(new Time(new Date().getTime()));
		values.add(new Time(new Date().getTime()));

		GenericDBWritable val = new GenericDBWritable(columns, values);
		LongWritable key = new LongWritable(1);

		Schema pairSchema = DBMapper.getPairSchema(val.getColumns());
		Schema keySchema = Pair.getKeySchema(pairSchema);

		GenericRecord keyRecord = new GenericData.Record(keySchema);
		keyRecord.put(0, key.get());
		assertEquals(keyRecord,mapper.getKeyRecord(keySchema, key));
	}
	
	@Test
	public void testGetValueRecord(){
		DBInputAvroMapper mapper = new DBInputAvroMapper();

		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ColumnInfo dateColumn = new ColumnInfo(1, Types.DATE, "dateColumn");
		ColumnInfo longColumn = new ColumnInfo(1, Types.BIGINT, "longColumn");
		ColumnInfo booleanColumn = new ColumnInfo(1, Types.BOOLEAN,
				"booleanColumn");
		ColumnInfo doubleColumn = new ColumnInfo(1, Types.DOUBLE,
				"doubleColumn");
		// ColumnInfo floatColumn = new ColumnInfo(1, Types.FLOAT,
		// "floatColumn");
		ColumnInfo charColumn = new ColumnInfo(1, Types.CHAR, "charColumn");
		ColumnInfo timeColumn = new ColumnInfo(1, Types.TIME, "timeColumn");
		ColumnInfo timeStampColumn = new ColumnInfo(1, Types.TIMESTAMP,
				"timeStampColumn");

		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();

		columns.add(intColumn);
		columns.add(stringColumn);
		columns.add(dateColumn);
		columns.add(longColumn);
		columns.add(booleanColumn);
		columns.add(doubleColumn);
		// columns.add(floatColumn);
		columns.add(charColumn);
		columns.add(timeColumn);
		columns.add(timeStampColumn);

		ArrayList values = new ArrayList();
		values.add(new Integer(12));
		values.add(new String("sam"));
		values.add(new Date());
		values.add(new Long(26564l));
		values.add(true);
		values.add(1.235);
		// values.add(new Float(1.0f));
		values.add('a');
		values.add(new Time(new Date().getTime()));
		values.add(new Time(new Date().getTime()));

		GenericDBWritable val = new GenericDBWritable(columns, values);
		LongWritable key = new LongWritable(1);

		Schema pairSchema = DBMapper.getPairSchema(val.getColumns());
		Schema keySchema = Pair.getKeySchema(pairSchema);
		Schema valueSchema = Pair.getValueSchema(pairSchema);
		
		GenericRecord valueRecord = new GenericData.Record(valueSchema);
		List<Schema.Field> fieldSchemas = valueSchema.getFields();
		for (int i = 0; i < val.getValues().size(); ++i) {
			Schema.Type type = fieldSchemas.get(i).schema().getType();
			if (type.equals(Schema.Type.STRING)) {
				Utf8 utf8 = new Utf8((String) val.getValues().get(i).toString());
				valueRecord.put(i, utf8);
			} else {
				valueRecord.put(i, val.getValues().get(i));
			}
		}
		
		assertEquals(valueRecord,mapper.getValueRecord(valueSchema, val));
	}

}
