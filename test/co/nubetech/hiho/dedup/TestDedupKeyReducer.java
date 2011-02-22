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

public class TestDedupKeyReducer {

	@Test
	public void testReducerValidValues() throws IOException,
			InterruptedException {
		Text key = new Text("key123");
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		String value1 = new String("value1");
		String value2 = new String("value2");
		String value3 = new String("value3");
		ArrayList<String> values = new ArrayList<String>();
		values.add(value1);
		values.add(value2);
		values.add(value3);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(DedupRecordCounter.OUTPUT);
		when(context.getCounter(DedupRecordCounter.OUTPUT)).thenReturn(counter);
		DedupKeyReducer dedupReducer = new DedupKeyReducer();
		dedupReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value1);
		assertEquals(1, context.getCounter(DedupRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerForLongWritableKey() throws IOException,
			InterruptedException {
		LongWritable key = new LongWritable(Long.parseLong("123"));
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		Text value1 = new Text("value1");
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(value1);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(DedupRecordCounter.OUTPUT);
		when(context.getCounter(DedupRecordCounter.OUTPUT)).thenReturn(counter);
		DedupKeyReducer dedupReducer = new DedupKeyReducer();
		dedupReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value1);
		assertEquals(1, context.getCounter(DedupRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerForBytesWritableKeyAndValue() throws IOException,
			InterruptedException {
		BytesWritable key = new BytesWritable("abc123".getBytes());
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		BytesWritable value1 = new BytesWritable("value1".getBytes());
		ArrayList<BytesWritable> values = new ArrayList<BytesWritable>();
		values.add(value1);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(DedupRecordCounter.OUTPUT);
		when(context.getCounter(DedupRecordCounter.OUTPUT)).thenReturn(counter);
		DedupKeyReducer dedupReducer = new DedupKeyReducer();
		dedupReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value1);
		assertEquals(1, context.getCounter(DedupRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerForIntWritableKeyAndValue() throws IOException,
			InterruptedException {
		IntWritable key = new IntWritable(123);
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		IntWritable value1 = new IntWritable(456);
		ArrayList<IntWritable> values = new ArrayList<IntWritable>();
		values.add(value1);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(DedupRecordCounter.OUTPUT);
		when(context.getCounter(DedupRecordCounter.OUTPUT)).thenReturn(counter);
		DedupKeyReducer dedupReducer = new DedupKeyReducer();
		dedupReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value1);
		assertEquals(1, context.getCounter(DedupRecordCounter.OUTPUT)
				.getValue());
	}

	@Test
	public void testReducerForNullValues() throws IOException,
			InterruptedException {
		Text key = new Text("key123");
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);

		String value1 = null;
		ArrayList<String> values = new ArrayList<String>();
		values.add(value1);

		Reducer.Context context = mock(Reducer.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(DedupRecordCounter.OUTPUT);
		when(context.getCounter(DedupRecordCounter.OUTPUT)).thenReturn(counter);
		DedupKeyReducer dedupReducer = new DedupKeyReducer();
		dedupReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value1);
		assertEquals(1, context.getCounter(DedupRecordCounter.OUTPUT)
				.getValue());
	}

}
