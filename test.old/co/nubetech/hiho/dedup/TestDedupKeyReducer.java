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

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
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
		DedupKeyReducer dedupReducer = new DedupKeyReducer();
		dedupReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value1);
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
		DedupKeyReducer dedupReducer = new DedupKeyReducer();
		dedupReducer.reduce(hihoTuple, values, context);
		verify(context).write(key, value1);
	}

}
