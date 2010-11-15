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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.mapreduce.lib.db.GenericDBWritable;

public class DBInputDelimMapper extends
		Mapper<LongWritable, GenericDBWritable, Text, Text> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.DBInputDelimMapper.class);

	private Text outkey;
	private Text outval;

	public DBInputDelimMapper() {
		outkey = new Text();
		outval = new Text();
	}

	public void map(LongWritable key, GenericDBWritable val, Context context)
			throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		logger.debug("Received val " + val);
		for (Object mapValue : val.getValues()) {
			builder.append(mapValue);
			builder.append(context.getConfiguration().get(
					HIHOConf.INPUT_OUTPUT_DELIMITER));
			logger.debug("Building delimited string " + builder);
		}
		// above loop will add extra delimiter in the end, deleting that
		int length = builder.length();
		if (length > 0)
			builder.deleteCharAt(length - 1);
		outval.set(builder.toString());
		context.write(outkey, outval);
	}

}
