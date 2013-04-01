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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import co.nubetech.hiho.dedup.HihoTuple;

public class TestMergeKeyMapper {
	@Test(expected = IOException.class)
	public final void testMapperForNullKeyValue() throws IOException,
			InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		Counters counters = new Counters();
		Counter counter = counters.findCounter(MergeRecordCounter.BAD_RECORD);
		when(context.getCounter(MergeRecordCounter.BAD_RECORD)).thenReturn(
				counter);
		MergeKeyMapper mapper = new MergeKeyMapper();
		Text val = new Text("valueOfKey");
		mapper.map(null, val, context);
	}

	@Test
	public final void testMapperValidValues() throws IOException,
			InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		Counters counters = new Counters();
		Counter counter = counters
				.findCounter(MergeRecordCounter.TOTAL_RECORDS_NEW);
		when(context.getCounter(MergeRecordCounter.TOTAL_RECORDS_NEW))
				.thenReturn(counter);

		MergeKeyMapper mapper = new MergeKeyMapper();
		Text key = new Text("abc123");
		Text val = new Text("valueOfKey");
		mapper.isOld = false;
		mapper.map(key, val, context);

		HihoValue hihoValue = new HihoValue();
		hihoValue.setVal(val);
		hihoValue.setIsOld(false);
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);
		verify(context).write(hihoTuple, hihoValue);
		assertEquals(1, context
				.getCounter(MergeRecordCounter.TOTAL_RECORDS_NEW).getValue());
	}

	/*@Test
	public final void testSetUp() throws IOException, InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		MergeKeyMapper mapper = new MergeKeyMapper();
		when(context.getInputSplit())
				.thenReturn(
						new FileSplit(new Path("old"), 2l, 14l,
								new String[] { "user" }));
		mapper.setup(context);
	}*/
}
