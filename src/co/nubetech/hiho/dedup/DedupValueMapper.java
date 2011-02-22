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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class DedupValueMapper<K, V extends Writable> extends
		Mapper<K, V, HihoTuple<V>, K> {
	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.DedupKeyMapper.class);

	@Override
	public void map(K key, V val, Context context) throws IOException,
			InterruptedException {
		if (key == null) {
			context.getCounter(DedupRecordCounter.BAD_RECORD).increment(1l);
			throw new IOException("Key is null");
		}
		context.getCounter(DedupRecordCounter.TOTAL_RECORDS_READ).increment(1l);
		HihoTuple<V> hihoTuple = new HihoTuple<V>();
		hihoTuple.setKey(val);
		context.write(hihoTuple, key);
	}

}
