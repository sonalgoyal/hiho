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
package co.nubetech.hiho.avro;

import static org.junit.Assert.assertEquals;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.mapred.Pair;
import org.junit.Test;

import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;

public class TestDBMapper {

	@Test
	public final void testGetColumnField() {
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

		DBMapper dBMapper = new DBMapper();

		Schema.Field intColumnField = new Schema.Field("intColumn",
				Schema.create(Schema.Type.INT), null, null);
		Schema.Field stringColumnField = new Schema.Field("stringColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field dateColumnField = new Schema.Field("dateColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field longColumnField = new Schema.Field("longColumn",
				Schema.create(Schema.Type.LONG), null, null);
		Schema.Field booleanColumnField = new Schema.Field("booleanColumn",
				Schema.create(Schema.Type.BOOLEAN), null, null);
		Schema.Field doubleColumnField = new Schema.Field("doubleColumn",
				Schema.create(Schema.Type.DOUBLE), null, null);
		Schema.Field charColumnField = new Schema.Field("charColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field timeColumnField = new Schema.Field("timeColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field timeStampColumnField = new Schema.Field("timeStampColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field floatColumnField = new Schema.Field("floatColumn",
				Schema.create(Schema.Type.DOUBLE), null, null);

		assertEquals(intColumnField, dBMapper.getColumnField(intColumn));
		assertEquals(stringColumnField, dBMapper.getColumnField(stringColumn));
		assertEquals(dateColumnField, dBMapper.getColumnField(dateColumn));
		assertEquals(longColumnField, dBMapper.getColumnField(longColumn));
		assertEquals(booleanColumnField, dBMapper.getColumnField(booleanColumn));
		assertEquals(doubleColumnField, dBMapper.getColumnField(doubleColumn));
		assertEquals(charColumnField, dBMapper.getColumnField(charColumn));
		assertEquals(timeColumnField, dBMapper.getColumnField(timeColumn));
		assertEquals(timeStampColumnField,
				dBMapper.getColumnField(timeStampColumn));
		assertEquals(floatColumnField, dBMapper.getColumnField(floatColumn));
	}

	@Test
	public final void testGetSchema() {
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

		DBMapper dBMapper = new DBMapper();
		Schema schema = dBMapper.getSchema(columns);

		// Computing actual values.
		Schema.Field intColumnField = new Schema.Field("intColumn",
				Schema.create(Schema.Type.INT), null, null);
		Schema.Field stringColumnField = new Schema.Field("stringColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field dateColumnField = new Schema.Field("dateColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field longColumnField = new Schema.Field("longColumn",
				Schema.create(Schema.Type.LONG), null, null);
		Schema.Field booleanColumnField = new Schema.Field("booleanColumn",
				Schema.create(Schema.Type.BOOLEAN), null, null);
		Schema.Field doubleColumnField = new Schema.Field("doubleColumn",
				Schema.create(Schema.Type.DOUBLE), null, null);
		Schema.Field charColumnField = new Schema.Field("charColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field timeColumnField = new Schema.Field("timeColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field timeStampColumnField = new Schema.Field("timeStampColumn",
				Schema.create(Schema.Type.STRING), null, null);
		Schema.Field floatColumnField = new Schema.Field("floatColumn",
				Schema.create(Schema.Type.DOUBLE), null, null);

		ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
		fields.add(intColumnField);
		fields.add(stringColumnField);
		fields.add(dateColumnField);
		fields.add(longColumnField);
		fields.add(booleanColumnField);
		fields.add(doubleColumnField);
		fields.add(charColumnField);
		fields.add(timeColumnField);
		fields.add(timeStampColumnField);
		fields.add(floatColumnField);

		Schema recordSchema = Schema.createRecord("hihoValue", null, null,
				false);
		recordSchema.setFields(fields);

		assertEquals(recordSchema, schema);
	}

	@Test
	public final void testGetPairSchema() {
		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);

		DBMapper dBMapper = new DBMapper();
		Schema schema = dBMapper.getPairSchema(columns);

		// Computing actual values.
		Schema longWritableSchema = Schema.createRecord("hihoKey", null, null,
				false);
		Schema.Field columnField = new Schema.Field("offset",
				Schema.create(Schema.Type.LONG), null, null);
		ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
		fields.add(columnField);
		longWritableSchema.setFields(fields);

		Schema pair = Schema.createRecord(Pair.class.getName(), null, null,
				false);
		List<Field> pairFields = new ArrayList<Field>();
		pairFields.add(new Field("key", longWritableSchema, "", null));
		pairFields.add(new Field("value", dBMapper.getSchema(columns), "",
				null, Field.Order.IGNORE));
		pair.setFields(pairFields);

		assertEquals(pair, schema);
	}

}
