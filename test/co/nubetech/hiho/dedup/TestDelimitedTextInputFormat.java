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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class TestDelimitedTextInputFormat {
	@Test
	public void testSetProperties() throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		DelimitedTextInputFormat.setProperties(job, ",", 1);
		assertEquals(
				",",
				job.getConfiguration().get(
						DelimitedTextInputFormat.DELIMITER_CONF));
		assertEquals("1",
				job.getConfiguration()
						.get(DelimitedTextInputFormat.COLUMN_CONF));
	}
}
