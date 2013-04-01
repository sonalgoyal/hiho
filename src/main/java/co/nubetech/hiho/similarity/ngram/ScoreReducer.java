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
package co.nubetech.hiho.similarity.ngram;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ScoreReducer extends
		Reducer<ValuePair, IntWritable, ValuePair, LongWritable> {
	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.similarity.ngram.ScoreReducer.class);
	
	@Override
	public void reduce(ValuePair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		if (key == null) {
			throw new IOException("Key is null");
		}
		logger.info("Key in ScoreReducer is: " + key);
		long valInLong = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			logger.info("Value in ScoreReducer is: " + iterator.next());
			valInLong = valInLong + 1l;
		}
		context.write(key, new LongWritable(valInLong));

	}
	
}
