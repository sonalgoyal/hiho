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

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestDelimitedLineRecordReader {
	@Test
	public void testGetColumnFixedSizeCols() throws IOException {
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
	public void testGetColumnVariableSize() throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abcde,cedd,efd,ghd,ijd,kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("ijd"), key);
		assertEquals(new Text("abcde,cedd,efd,ghd,ijd,kld"), val);
	}

	@Test
	public void testGetColumnForDelimiterSizeGreaterThanOne()
			throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abd::cdd::efd::ghd::ijd::kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, "::");
		System.out.println("key is: " + key);
		assertEquals(new Text("ijd"), key);
		assertEquals(new Text("abd::cdd::efd::ghd::ijd::kld"), val);
	}

	@Test
	public void testGetColumnStartingWithDelimiter() throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text(",abd,cdd,efd,ghd,ijd,kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("ghd"), key);
		assertEquals(new Text(",abd,cdd,efd,ghd,ijd,kld"), val);
	}

	@Test(expected = IOException.class)
	public void testGetColumnForNullDelimiter() throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abd::cdd::efd::ghd::ijd::kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, null);
	}

	@Test(expected = IOException.class)
	public void testGetColumnForEmptyDelimiter() throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abd::cdd::efd::ghd::ijd::kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, "");
	}

	@Test
	public void testGetColumnHavingSpecialCharacterInValue() throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("ab$d,cdd,efd,ghd,i%jd,kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("i%jd"), key);
		assertEquals(new Text("ab$d,cdd,efd,ghd,i%jd,kld"), val);
	}

	@Test
	public void testGetColumnHavingQuotesInValue() throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("ab\"d,cdd,efd,ghd,i'jd,kl~d");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("i'jd"), key);
		assertEquals(new Text("ab\"d,cdd,efd,ghd,i'jd,kl~d"), val);
	}

	@Test
	public void testGetColumnHavingBackSlashInValue() throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("ab\"d,cdd,efd,ghd,i\\'jd,kl~d");
		Text key = delimitedLineRecordReader.getColumn(val, 5, ",");
		System.out.println("key is: " + key);
		assertEquals(new Text("i\\'jd"), key);
		assertEquals(new Text("ab\"d,cdd,efd,ghd,i\\'jd,kl~d"), val);
	}

	@Test
	public void testGetColumnHavingSpecialCharacterDelimiterValue()
			throws IOException {
		DelimitedLineRecordReader delimitedLineRecordReader = new DelimitedLineRecordReader();
		Text val = new Text("abd$cdd$efd$ghd$ijd$kld");
		Text key = delimitedLineRecordReader.getColumn(val, 5, "$");
		System.out.println("key is: " + key);
		assertEquals(new Text("ijd"), key);
		assertEquals(new Text("abd$cdd$efd$ghd$ijd$kld"), val);
	}
}
