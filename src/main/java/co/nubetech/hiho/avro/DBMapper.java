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

package co.nubetech.hiho.avro;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.mapred.Pair;
import org.apache.log4j.Logger;

import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;

public class DBMapper {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.avro.DBMapper.class);

	public static Schema getSchema(ArrayList<ColumnInfo> columnInfo) {
		ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
		for (ColumnInfo column : columnInfo) {

			Schema.Field columnField = getColumnField(column);
			logger.debug("Schema for field " + column + " "
					+ columnField.toString());
			fields.add(columnField);
		}
		Schema recordSchema = Schema.createRecord("hihoValue", null, null,
				false);
		recordSchema.setFields(fields);
		return recordSchema;
	}

	public static Schema getPairSchema(ArrayList<ColumnInfo> columnInfo) {
		Schema pair = Schema.createRecord(Pair.class.getName(), null, null,
				false);
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("key", getLongWritableSchema(), "", null));
		fields.add(new Field("value", getSchema(columnInfo), "", null,
				Field.Order.IGNORE));
		pair.setFields(fields);
		return pair;
	}

	public static Schema getLongWritableSchema() {
		Schema recordSchema = Schema.createRecord("hihoKey", null, null, false);
		Schema.Field columnField = new Schema.Field("offset",
				Schema.create(Schema.Type.LONG), null, null);
		ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
		fields.add(columnField);
		recordSchema.setFields(fields);
		return recordSchema;
	}

	public static Schema.Field getColumnField(ColumnInfo column) {
		Schema fieldSchema = getSchema(column.getType());
		Schema.Field field = new Schema.Field(column.getName(), fieldSchema,
				null, null);
		return field;
	}

	// TODO: Verify mappings based on db definitions
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
