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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestHashUtility {

	@Test
	public void testMD5HashForTextValueValue() throws IOException {
		Text key = new Text("abc2hf123");
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForStringValue() throws IOException {
		String key = new String("abc2hf123");
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForLongWritableKey() throws IOException {
		LongWritable key = new LongWritable(Long.parseLong("123"));
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForBytesWritableKey() throws IOException {
		BytesWritable key = new BytesWritable("abc123".getBytes());
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForIntWritableKey() throws IOException {
		IntWritable key = new IntWritable(123);
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForArrayWritableKey() throws IOException {
		ArrayWritable key = new ArrayWritable(new String[] { "abc123" });
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForBooleanWritableKey() throws IOException {
		BooleanWritable key = new BooleanWritable(true);
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForFloatWritableKey() throws IOException {
		FloatWritable key = new FloatWritable(1.025f);
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForByteWritableKey() throws IOException {
		ByteWritable key = new ByteWritable(new Byte("123"));
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}

	@Test
	public void testMD5HashForDoubleWritableKey() throws IOException {
		DoubleWritable key = new DoubleWritable(123d);
		MD5Hash md5HashKey1 = HashUtility.getMD5Hash(key);
		MD5Hash md5HashKey2 = HashUtility.getMD5Hash(key);
		assertEquals(md5HashKey1, md5HashKey2);
	}
}
