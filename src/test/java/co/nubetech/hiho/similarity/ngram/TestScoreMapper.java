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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

public class TestScoreMapper {
	
	@Test
	public final void testMapperValidValues() throws IOException, InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		ScoreMapper mapper = new ScoreMapper();
		ValuePair valuePair = new ValuePair();
		valuePair.setValue1(new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		valuePair.setValue2(new Text("This is not a bookdelimiterBetweenKeyAndValuevalue2"));
		mapper.map(valuePair, new IntWritable(1), context);
		verify(context).write(valuePair, new IntWritable(1));		
	}
	
	@Test(expected = IOException.class)
	public final void testMapperForNullValues() throws IOException, InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		ScoreMapper mapper = new ScoreMapper();
		mapper.map(null, new IntWritable(1), context);	
	}
}
