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
}
