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
package co.nubetech.hiho.merge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

import co.nubetech.hiho.dedup.HihoTuple;

public class MergeValueReducer<K, V> extends
		Reducer<HihoTuple, HihoValue, K, V> {
	@Override
	public void reduce(HihoTuple hihoTuple, Iterable<HihoValue> hihoValues,
			Context context) throws IOException, InterruptedException {
		context.getCounter(MergeRecordCounter.OUTPUT).increment(1l);
		V val = (V) hihoTuple.getKey();
		Iterator<HihoValue> iterator = hihoValues.iterator();
		if (hihoValues == null || hihoValues.equals(null)
				|| !iterator.hasNext()) {
			context.write(null, val);
		} else {
			K key = null;
			HihoValue hihoValue = null;
			while (iterator.hasNext()) {
				hihoValue = iterator.next();
				if (!hihoValue.getIsOld()) {
					key = (K) hihoValue.getVal();
				}
			}
			if (key == null) {
				key = (K) hihoValue.getVal();
			}
			context.write(key, val);

		}
	}

}
