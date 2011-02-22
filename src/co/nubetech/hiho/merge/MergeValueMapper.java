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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.dedup.HihoTuple;

public class MergeValueMapper<K extends Writable, V extends Writable> extends
		Mapper<K, V, HihoTuple<V>, HihoValue<K>> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.DedupKeyMapper.class);
	protected Boolean isOld;

	@Override
	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		InputSplit is = context.getInputSplit();
		FileSplit fs = (FileSplit) is;
		Path splitPath = fs.getPath();

		if (splitPath.toString().contains(conf.get(HIHOConf.MERGE_OLD_PATH))) {
			isOld = true;
		} else if (splitPath.toString().contains(
				conf.get(HIHOConf.MERGE_NEW_PATH))) {
			isOld = false;
		} else {
			throw new IOException("File " + splitPath
					+ " is not under new path"
					+ conf.get(HIHOConf.MERGE_NEW_PATH) + " and old path"
					+ conf.get(HIHOConf.MERGE_OLD_PATH));
		}
	}

	@Override
	public void map(K key, V val, Context context) throws IOException,
			InterruptedException {
		if (key == null) {
			context.getCounter(MergeRecordCounter.BAD_RECORD).increment(1l);
			throw new IOException("Key is null");
		}
		if (isOld) {
			context.getCounter(MergeRecordCounter.TOTAL_RECORDS_OLD).increment(
					1l);
		} else {
			context.getCounter(MergeRecordCounter.TOTAL_RECORDS_NEW).increment(
					1l);
		}
		HihoValue<K> hihoValue = new HihoValue<K>();
		hihoValue.setVal(key);
		hihoValue.setIsOld(isOld);
		HihoTuple<V> hihoTuple = new HihoTuple<V>();
		hihoTuple.setKey(val);
		context.write(hihoTuple, hihoValue);
	}

}
