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

public class TestHihoTuple {

	@Test
	public void testSetKey() throws IOException {
		Text key = new Text("abc123");
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setKey(key);
		assertEquals(key, hihoTuple.getKey());
		assertEquals(HashUtility.getMD5Hash(key), hihoTuple.getHash());
	}

	@Test
	public void testSetMD5Hash() throws IOException {
		Text key = new Text("abc123");
		MD5Hash hash = HashUtility.getMD5Hash(key);
		HihoTuple hihoTuple = new HihoTuple();
		hihoTuple.setHash(hash);
		assertEquals(hash, hihoTuple.getHash());
	}

}
