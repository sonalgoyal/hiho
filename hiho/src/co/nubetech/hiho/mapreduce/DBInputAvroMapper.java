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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import co.nubetech.hiho.avro.DBMapper;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class DBInputAvroMapper extends
		Mapper<LongWritable, GenericDBWritable, GenericRecord, Text> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.DBInputAvroMapper.class);

	private Schema schema = null;
	GenericDatumWriter<GenericRecord> writer;
	private Text outkey;

	public void map(LongWritable key, GenericDBWritable val, Context context)
			throws IOException, InterruptedException {
		if (schema == null) {
			logger.debug("Generating schema");
			schema = DBMapper.getSchema(val.getColumns());
			writer = new GenericDatumWriter<GenericRecord>(schema);
			logger.debug("Schema is " + schema.toString(true));
		}
		// now generate the avro record
		GenericRecord record = new GenericData.Record(schema);
		List<Schema.Field> fieldSchemas = schema.getFields();
		for (int i = 0; i < val.getValues().size(); ++i) {
			Schema.Type type = fieldSchemas.get(i).schema().getType();
			if (type.equals(Schema.Type.STRING)) {
				Utf8 utf8 = new Utf8((String) val.getValues().get(i));
				record.put(i, utf8);
			} else {
				record.put(i, val.getValues().get(i));
			}
		}
		logger.debug("Record is " + record);
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		writer.write(record, new BinaryEncoder(stream));
		outkey = new Text();
		outkey.set(new String(stream.toByteArray()));
		stream.close();
		context.write(record, outkey);
	}

}
