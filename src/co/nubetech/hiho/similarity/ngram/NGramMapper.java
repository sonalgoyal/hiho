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
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class NGramMapper extends Mapper<Text, Text, Text, Text> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.similarity.ngram.NGramMapper.class);

	@Override
	public void map(Text key, Text val, Context context) throws IOException,
			InterruptedException {
		if (key == null) {
			throw new IOException("Key is null");
		}
		HashSet<String> nGramList = new HashSet<String>();
		int gramSize = 2;
		nGramList = getNGrams(key, gramSize);
		for (String nGrams : nGramList) {
			String value = key.toString() + "delimiterBetweenKeyAndValue" + val.toString();
			context.write(new Text(nGrams), new Text(value));
			logger.info("Key and Value in NGram Mapper is: " + new Text(nGrams)
					+ ", " + new Text(value));
		}
	}

	public HashSet<String> getNGrams(Text line, int gramSize) {
		ArrayList<String> words = new ArrayList<String>();
		HashSet<String> nGrams = new HashSet<String>();
		String[] tokens = line.toString().split(" ");
		for (String t : tokens) {
			words.add(t);
		}
		for (int i = 0; i < words.size() - gramSize + 1; i++) {
			String key = "";
			for (int j = i; j < i + gramSize; j++) {
				key += words.get(j);
				if(j != ( i + gramSize - 1)){
				key += " ";
				}
			}
			nGrams.add(key);
		}
		return nGrams;
	}
	
}
