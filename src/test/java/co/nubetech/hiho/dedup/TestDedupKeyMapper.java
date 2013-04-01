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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

public class TestDedupKeyMapper {

	@Test
	public final void testMapperValidValues() throws IOException,
			InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		Counters counters = new Counters();
		Counter counter = counters
				.findCounter(DedupRecordCounter.TOTAL_RECORDS_READ);
		when(context.getCounter(DedupRecordCounter.TOTAL_RECORDS_READ))
				.thenReturn(counter);
		DedupKeyMapper<Text, String> mapper = new DedupKeyMapper<Text, String>();
		Text key = new Text("abc123");
		String val = "valueOfKey";
		mapper.map(key, val, context);

		HihoTuple<Text> hihoTuple = new HihoTuple<Text>();
		hihoTuple.setKey(key);
		verify(context).write(hihoTuple, val);
		assertEquals(1,
				context.getCounter(DedupRecordCounter.TOTAL_RECORDS_READ)
						.getValue());
	}

	@Test(expected = IOException.class)
	public final void testMapperForNullKeyValue() throws IOException,
			InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(DedupRecordCounter.BAD_RECORD);
		when(context.getCounter(DedupRecordCounter.BAD_RECORD)).thenReturn(
				counter);
		DedupKeyMapper mapper = new DedupKeyMapper();
		String val = "valueOfKey";
		mapper.map(null, val, context);
	}

}
