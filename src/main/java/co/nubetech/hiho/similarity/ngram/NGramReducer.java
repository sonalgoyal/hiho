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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class NGramReducer extends Reducer<Text, Text, ValuePair, IntWritable> {
	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.similarity.ngram.NGramReducer.class);

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		if (key == null) {
			throw new IOException("Key is null");
		}

		logger.info("Key in NGram Reducer is: " + key);

		ArrayList<Text> value = new ArrayList<Text>();
		
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			Text valueInIterator = iterator.next();
			logger.info("Value in NGram Reducer is: " + valueInIterator);
			value.add(new Text(valueInIterator));
		}

		for (Text valueInList : value) {
			logger.info("Value added in list is: " + valueInList);
		}

		for (int i = 0; i < value.size() - 1; i++) {
			for (int j = i + 1; j < value.size(); j++) {
				ValuePair valuePair = new ValuePair();
				valuePair.setValue1(value.get(i));
				valuePair.setValue2(value.get(j));
				logger.info("Value set in ValuePair is: " + value.get(i) + ", "
						+ value.get(j));
				context.write(valuePair, new IntWritable(1));
			}

		}
	}
}
