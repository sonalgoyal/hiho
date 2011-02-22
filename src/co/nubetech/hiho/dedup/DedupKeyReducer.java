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
package co.nubetech.hiho.dedup;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class DedupKeyReducer<K extends Writable, V> extends
		Reducer<HihoTuple<K>, V, K, V> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.DedupKeyReducer.class);

	@Override
	public void reduce(HihoTuple<K> hihoTuple, Iterable<V> values,
			Context context) throws IOException, InterruptedException {
		context.getCounter(DedupRecordCounter.OUTPUT).increment(1l);
		K key = (K) hihoTuple.getKey();
		logger.debug("Key emitting in reducer is: " + key);
		// When values is null, emit key and null.
		if (values.equals(null)) {
			context.write(key, null);
		} else {
			V val = values.iterator().next();
			logger.debug("Value emitting in reducer is: " + val);
			context.write(key, val);
		}
	}

}
