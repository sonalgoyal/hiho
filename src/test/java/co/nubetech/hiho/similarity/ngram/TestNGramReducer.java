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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

public class TestNGramReducer {
	
	@Test(expected = IOException.class)
	public final void testReducerForNullValues() throws IOException, InterruptedException {
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		values.add(new Text("This is not a bookdelimiterBetweenKeyAndValuevalue2"));
		Reducer.Context context = mock(Reducer.Context.class);
		NGramReducer reducer = new NGramReducer();
		reducer.reduce(null, values, context);
	}
	
	@Test
	public void testReducerValidValues() throws IOException, InterruptedException {
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		values.add(new Text("This is not a bookdelimiterBetweenKeyAndValuevalue2"));
		Reducer.Context context = mock(Reducer.Context.class);
		NGramReducer reducer = new NGramReducer();
		reducer.reduce(new Text("This is"), values, context);
		ValuePair valuePair = new ValuePair();
		valuePair.setValue1(new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		valuePair.setValue2(new Text("This is not a bookdelimiterBetweenKeyAndValuevalue2"));
		verify(context).write(valuePair, new IntWritable(1));		
	}

}
