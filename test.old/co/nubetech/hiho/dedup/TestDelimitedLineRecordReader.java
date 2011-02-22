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

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestDelimitedLineRecordReader {
	@Test
	public void testGetColumnFixedSizeCols() {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abd,cdd,efd,ghd,ijd,kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("ijd"), key);
		assertEquals(new Text("abd,cdd,efd,ghd,ijd,kld"), val); // checking
																// parsed text
																// value
	}

	@Test
	public void testGetColumnVariableSize() {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abcde,cedd,efd,ghd,ijd,kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("ijd"), key);
		assertEquals(new Text("abcde,cedd,efd,ghd,ijd,kld"), val);
	}

	@Test
	public void testGetColumnForDelimiterSizeGreaterThanOne() {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abd::cdd::efd::ghd::ijd::kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, "::");
		System.out.println("key is: " + key);
		assertEquals(new Text("ijd"), key);
		assertEquals(new Text("abd::cdd::efd::ghd::ijd::kld"), val);
	}

	@Test
	public void testGetColumnStartingWithDelimiter() {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text(",abd,cdd,efd,ghd,ijd,kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("ghd"), key);
		assertEquals(new Text(",abd,cdd,efd,ghd,ijd,kld"), val);
	}
}
