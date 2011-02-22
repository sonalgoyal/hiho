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

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import co.nubetech.hiho.avro.DBMapper;
import co.nubetech.hiho.mapreduce.lib.db.ColumnInfo;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class DBInputAvroMapper extends MapReduceBase implements
		Mapper<LongWritable, GenericDBWritable, AvroValue<Pair>, NullWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.DBInputAvroMapper.class);

	GenericDatumWriter<Pair> writer;
	private NullWritable nullWritable = NullWritable.get();
	Pair pair = null;
	Schema pairSchema = null;
	Schema keySchema = null;
	Schema valueSchema = null;

	@Override
	public void map(LongWritable key, GenericDBWritable val,
			OutputCollector<AvroValue<Pair>, NullWritable> output, 
			Reporter reporter) throws IOException {
		
		logger.debug("Key, val are " + key + " val " + val.getColumns());
		
		if (pairSchema == null) {
			logger.debug("Creating schema for MR");
			logger.debug("MR columns are " + val.getColumns());
			for (ColumnInfo column: val.getColumns()) {
				logger.debug("Column is " + column.getIndex() + " " + column.getName());
			}
			pairSchema = DBMapper.getPairSchema(val.getColumns());
			keySchema = Pair.getKeySchema(pairSchema);
			valueSchema = Pair.getValueSchema(pairSchema);
			pair = new Pair<GenericRecord, GenericRecord>(pairSchema);
		}

		

		// writer = new GenericDatumWriter<Pair>(pairSchema);
		
		GenericRecord keyRecord =this.getKeyRecord(keySchema, key);

		logger.debug("Key record is " + keyRecord);
		// now generate the avro record
		GenericRecord valueRecord = this.getValueRecord(valueSchema, val);
		logger.debug("Value Record is " + valueRecord);
		/*
		 * ByteArrayOutputStream stream = new ByteArrayOutputStream();
		 * writer.write(record, new BinaryEncoder(stream)); stream.close();
		 */
		pair.key(keyRecord);
		pair.value(valueRecord);
		output.collect(new AvroValue<Pair>(pair), nullWritable );
	}

	public GenericRecord getKeyRecord(Schema  keySchema, LongWritable key) {
		GenericRecord keyRecord = new GenericData.Record(keySchema);
		keyRecord.put(0, key.get());
		return keyRecord;
	}

	public GenericRecord getValueRecord(Schema valueSchema,
			GenericDBWritable val) {
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
		return valueRecord;
	}

}
