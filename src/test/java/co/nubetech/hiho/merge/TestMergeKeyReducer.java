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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import co.nubetech.hiho.dedup.HihoTuple;

public class TestMergeKeyReducer {
	@Test
	public void testReducerValidValues() throws IOException,
			InterruptedException {
		Text key = new Text("key123");
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		HihoValue hihoValue1 = new HihoValue();
		HihoValue hihoValue2 = new HihoValue();
		Text value1 = new Text("value1");
		Text value2 = new Text("value2");
		hihoValue1.setVal(value1);
		hihoValue2.setVal(value2);
		hihoValue1.setIsOld(true);
		hihoValue2.setIsOld(false);
		ArrayList<HihoValue> values = new ArrayList<HihoValue>();
		values.add(hihoValue1);
		values.add(hihoValue2);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(MergeRecordCounter.OUTPUT);
		when(context.getCounter(MergeRecordCounter.OUTPUT)).thenReturn(counter);
		MergeKeyReducer mergeReducer = new MergeKeyReducer();
		mergeReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value2);
		assertEquals(1, context.getCounter(MergeRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerNullValues() throws IOException,
			InterruptedException {
		Text key = new Text("key123");
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		HihoValue hihoValue1 = new HihoValue();
		HihoValue hihoValue2 = new HihoValue();
		Text value1 = new Text("value1");
		Text value2 = new Text("value2");
		hihoValue1.setVal(value1);
		hihoValue2.setVal(value2);
		hihoValue1.setIsOld(true);
		hihoValue2.setIsOld(false);
		ArrayList<HihoValue> values = new ArrayList<HihoValue>();

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(MergeRecordCounter.OUTPUT);
		when(context.getCounter(MergeRecordCounter.OUTPUT)).thenReturn(counter);
		MergeKeyReducer mergeReducer = new MergeKeyReducer();
		mergeReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, null);
		assertEquals(1, context.getCounter(MergeRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerForLongWritableKey() throws IOException,
			InterruptedException {
		LongWritable key = new LongWritable(Long.parseLong("123"));
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		HihoValue hihoValue1 = new HihoValue();
		HihoValue hihoValue2 = new HihoValue();
		Text value1 = new Text("value1");
		Text value2 = new Text("value2");
		hihoValue1.setVal(value1);
		hihoValue2.setVal(value2);
		hihoValue1.setIsOld(true);
		hihoValue2.setIsOld(false);
		ArrayList<HihoValue> values = new ArrayList<HihoValue>();
		values.add(hihoValue1);
		values.add(hihoValue2);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(MergeRecordCounter.OUTPUT);
		when(context.getCounter(MergeRecordCounter.OUTPUT)).thenReturn(counter);
		MergeKeyReducer mergeReducer = new MergeKeyReducer();
		mergeReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value2);
		assertEquals(1, context.getCounter(MergeRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerForBytesWritableKeyAndValue() throws IOException,
			InterruptedException {
		BytesWritable key = new BytesWritable("abc123".getBytes());
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		HihoValue hihoValue1 = new HihoValue();
		HihoValue hihoValue2 = new HihoValue();
		BytesWritable value1 = new BytesWritable("value1".getBytes());
		BytesWritable value2 = new BytesWritable("value2".getBytes());
		hihoValue1.setVal(value1);
		hihoValue2.setVal(value2);
		hihoValue1.setIsOld(true);
		hihoValue2.setIsOld(false);
		ArrayList<HihoValue> values = new ArrayList<HihoValue>();
		values.add(hihoValue1);
		values.add(hihoValue2);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(MergeRecordCounter.OUTPUT);
		when(context.getCounter(MergeRecordCounter.OUTPUT)).thenReturn(counter);
		MergeKeyReducer mergeReducer = new MergeKeyReducer();
		mergeReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value2);
		assertEquals(1, context.getCounter(MergeRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerForIntWritableKeyAndValue() throws IOException,
			InterruptedException {
		IntWritable key = new IntWritable(123);
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		HihoValue hihoValue1 = new HihoValue();
		HihoValue hihoValue2 = new HihoValue();
		IntWritable value1 = new IntWritable(456);
		IntWritable value2 = new IntWritable(789);
		hihoValue1.setVal(value1);
		hihoValue2.setVal(value2);
		hihoValue1.setIsOld(true);
		hihoValue2.setIsOld(false);
		ArrayList<HihoValue> values = new ArrayList<HihoValue>();
		values.add(hihoValue1);
		values.add(hihoValue2);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(MergeRecordCounter.OUTPUT);
		when(context.getCounter(MergeRecordCounter.OUTPUT)).thenReturn(counter);
		MergeKeyReducer mergeReducer = new MergeKeyReducer();
		mergeReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value2);
		assertEquals(1, context.getCounter(MergeRecordCounter.OUTPUT)
				.getValue());
	}

}
