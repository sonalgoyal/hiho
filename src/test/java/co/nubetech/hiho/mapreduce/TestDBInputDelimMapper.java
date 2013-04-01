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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class TestDBInputDelimMapper {
	@Test
	public final void testMapperValidValues() throws IOException,
			InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		Configuration conf = new Configuration();
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		when(context.getConfiguration()).thenReturn(conf);

		DBInputDelimMapper mapper = new DBInputDelimMapper();

		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ColumnInfo dateColumn = new ColumnInfo(1, Types.DATE, "dateColumn");
		ColumnInfo longColumn = new ColumnInfo(1, Types.BIGINT, "longColumn");
		ColumnInfo booleanColumn = new ColumnInfo(1, Types.BOOLEAN,
				"booleanColumn");
		ColumnInfo doubleColumn = new ColumnInfo(1, Types.DOUBLE,
				"doubleColumn");
		ColumnInfo charColumn = new ColumnInfo(1, Types.CHAR, "charColumn");
		ColumnInfo timeColumn = new ColumnInfo(1, Types.TIME, "timeColumn");
		ColumnInfo timeStampColumn = new ColumnInfo(1, Types.TIMESTAMP,
				"timeStampColumn");
		ColumnInfo floatColumn = new ColumnInfo(1, Types.FLOAT, "floatColumn");

		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		columns.add(dateColumn);
		columns.add(longColumn);
		columns.add(booleanColumn);
		columns.add(doubleColumn);
		columns.add(charColumn);
		columns.add(timeColumn);
		columns.add(timeStampColumn);
		columns.add(floatColumn);

		ArrayList<Comparable> values = new ArrayList<Comparable>();
		values.add(new Integer(12));
		values.add(new String("sam"));
		values.add(new Date());
		values.add(new Long(26564l));
		values.add(true);
		values.add(1.235);
		values.add('a');
		values.add(new Time(new Date().getTime()));
		values.add(new Time(new Date().getTime()));
		values.add(new Float(1.0f));

		GenericDBWritable val = new GenericDBWritable(columns, values);
		LongWritable key = new LongWritable(1);
		mapper.map(key, val, context);

		Text outkey = new Text();
		Text outval = new Text();
		StringBuilder builder = new StringBuilder();
		builder.append(new Integer(12) + "," + new String("sam") + ","
				+ new Date() + "," + new Long(26564l) + "," + true + ","
				+ 1.235 + "," + 'a' + "," + new Time(new Date().getTime())
				+ "," + new Time(new Date().getTime()) + "," + new Float(1.0f));

		outval.set(builder.toString());
		verify(context).write(outkey, outval);
	}
	
	@Test
	public final void testMapperValidValuesDelmiter() throws IOException,
			InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		Configuration conf = new Configuration();
		String delimiter = "DELIM";
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, delimiter);
		when(context.getConfiguration()).thenReturn(conf);

		DBInputDelimMapper mapper = new DBInputDelimMapper();

		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
				"stringColumn");
		ColumnInfo dateColumn = new ColumnInfo(1, Types.DATE, "dateColumn");
		ColumnInfo longColumn = new ColumnInfo(1, Types.BIGINT, "longColumn");
		ColumnInfo booleanColumn = new ColumnInfo(1, Types.BOOLEAN,
				"booleanColumn");
		ColumnInfo doubleColumn = new ColumnInfo(1, Types.DOUBLE,
				"doubleColumn");
		ColumnInfo charColumn = new ColumnInfo(1, Types.CHAR, "charColumn");
		ColumnInfo timeColumn = new ColumnInfo(1, Types.TIME, "timeColumn");
		ColumnInfo timeStampColumn = new ColumnInfo(1, Types.TIMESTAMP,
				"timeStampColumn");
		ColumnInfo floatColumn = new ColumnInfo(1, Types.FLOAT, "floatColumn");

		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		columns.add(stringColumn);
		columns.add(dateColumn);
		columns.add(longColumn);
		columns.add(booleanColumn);
		columns.add(doubleColumn);
		columns.add(charColumn);
		columns.add(timeColumn);
		columns.add(timeStampColumn);
		columns.add(floatColumn);

		ArrayList<Comparable> values = new ArrayList<Comparable>();
		values.add(new Integer(12));
		values.add(new String("sam"));
		values.add(new Date());
		values.add(new Long(26564l));
		values.add(true);
		values.add(1.235);
		values.add('a');
		values.add(new Time(new Date().getTime()));
		values.add(new Time(new Date().getTime()));
		values.add(new Float(1.0f));

		GenericDBWritable val = new GenericDBWritable(columns, values);
		LongWritable key = new LongWritable(1);
		mapper.map(key, val, context);

		Text outkey = new Text();
		Text outval = new Text();
		StringBuilder builder = new StringBuilder();
		builder.append(new Integer(12) + delimiter + new String("sam") + delimiter
				+ new Date() + delimiter + new Long(26564l) + delimiter + true + delimiter
				+ 1.235 + delimiter + 'a' + delimiter + new Time(new Date().getTime())
				+ delimiter + new Time(new Date().getTime()) + delimiter + new Float(1.0f));

		outval.set(builder.toString());
		verify(context).write(outkey, outval);
	}

	@Test
	public final void testMapperNullValues() throws IOException,
			InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		Configuration conf = new Configuration();
		conf.set(HIHOConf.INPUT_OUTPUT_DELIMITER, ",");
		when(context.getConfiguration()).thenReturn(conf);

		DBInputDelimMapper mapper = new DBInputDelimMapper();
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		ArrayList values = new ArrayList();
		GenericDBWritable val = new GenericDBWritable(columns, values);
		LongWritable key = new LongWritable(1);
		mapper.map(key, val, context);

		Text outkey = new Text();
		Text outval = new Text();
		verify(context).write(outkey, outval);
	}
}
