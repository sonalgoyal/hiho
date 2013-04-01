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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

public class TestNGramMapper {
	
	@Test(expected = IOException.class)
	public final void testMapperForNullValues() throws IOException, InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		NGramMapper mapper = new NGramMapper();
		mapper.map(null, new Text("value1"), context);	
	}
	
	@Test
	public final void testMapperForValidValues() throws IOException, InterruptedException {
		Mapper.Context context = mock(Mapper.Context.class);
		NGramMapper mapper = new NGramMapper();
		mapper.map(new Text("This is a book"), new Text("value1"), context);
		verify(context).write(new Text("a book"), new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		verify(context).write(new Text("This is"), new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		verify(context).write(new Text("is a"), new Text("This is a bookdelimiterBetweenKeyAndValuevalue1"));
		}

}
