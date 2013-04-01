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
package co.nubetech.hiho.testdata;

import java.io.File;
import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;

public class AvroDataFileWriteDemo {

	public static void main(String[] args) throws IOException {

		ColumnInfo intColumn = new ColumnInfo(0, Types.INTEGER, "intColumn");
		// ColumnInfo stringColumn = new ColumnInfo(1, Types.VARCHAR,
		// "stringColumn");
		ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
		columns.add(intColumn);
		// columns.add(stringColumn);

		File file = new File("data.avro");
		Schema schema = getSchema(columns);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				writer);
		dataFileWriter.create(schema, file);

		GenericRecord datum = new GenericData.Record(schema);
		datum.put("intColumn", new Integer(12));
		// datum.put("stringColumn", new String("sam"));

		dataFileWriter.append(datum);
		dataFileWriter.close();
	}

	public static Schema getSchema(ArrayList<ColumnInfo> columnInfo) {
		ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
		for (ColumnInfo column : columnInfo) {

			Schema.Field columnField = getColumnField(column);
			fields.add(columnField);
		}
		Schema recordSchema = Schema.createRecord("hihoValue", null, null,
				false);
		recordSchema.setFields(fields);
		return recordSchema;
	}

	public static Schema.Field getColumnField(ColumnInfo column) {
		Schema fieldSchema = getSchema(column.getType());
		Schema.Field field = new Schema.Field(column.getName(), fieldSchema,
				null, null);
		return field;
	}

	public static Schema getSchema(int type) {
		Schema.Type returnType = null;
		switch (type) {
		case Types.ARRAY:
			returnType = Schema.Type.ARRAY;
			break;
		case Types.BIGINT:
			returnType = Schema.Type.LONG;
			break;
		case Types.BINARY:
		case Types.BIT:
		case Types.BLOB:
			returnType = Schema.Type.BYTES;
			break;
		case Types.BOOLEAN:
			returnType = Schema.Type.BOOLEAN;
			break;
		case Types.CHAR:
			returnType = Schema.Type.STRING;
			break;
		case Types.CLOB:
			returnType = Schema.Type.BYTES;
			break;
		// case Types.DATALINK
		case Types.DATE:
			returnType = Schema.Type.STRING;
			break;
		case Types.DECIMAL:
			returnType = Schema.Type.DOUBLE;
			break;
		// case Types.DISTINCT
		case Types.DOUBLE:
		case Types.FLOAT:
			returnType = Schema.Type.DOUBLE;
			break;
		case Types.INTEGER:
			returnType = Schema.Type.INT;
			break;
		// case Types.JAVA_OBJECT
		case Types.LONGVARBINARY:
			returnType = Schema.Type.BYTES;
			break;
		case Types.LONGVARCHAR:
			returnType = Schema.Type.STRING;
			break;
		case Types.NULL:
			returnType = Schema.Type.NULL;
			break;
		case Types.NUMERIC:
			returnType = Schema.Type.DOUBLE;
			break;
		// case Types.OTHER
		case Types.REAL:
			returnType = Schema.Type.FLOAT;
			break;
		// case Types.REF
		case Types.SMALLINT:
			returnType = Schema.Type.INT;
			break;
		// case Types.STRUCT
		case Types.TIME:
			returnType = Schema.Type.STRING;
			break;
		case Types.TIMESTAMP:
			returnType = Schema.Type.STRING;
			break;
		case Types.TINYINT:
			returnType = Schema.Type.INT;
			break;
		case Types.VARBINARY:
			returnType = Schema.Type.BYTES;
			break;
		case Types.VARCHAR:
			returnType = Schema.Type.STRING;
			break;
		default:
			returnType = null;
			break;
		}
		Schema returnSchema = Schema.create(returnType);
		return returnSchema;
	}
}
