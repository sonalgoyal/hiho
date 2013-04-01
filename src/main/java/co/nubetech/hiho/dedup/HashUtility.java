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
import org.apache.log4j.Logger;

public class HashUtility {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.HashUtility.class);

	public static MD5Hash getMD5Hash(Object object) throws IOException {
		return MD5Hash.digest(object.toString());
	}

	public static MD5Hash getMD5Hash(Text key) throws IOException {
		return MD5Hash.digest(key.getBytes());
	}

	public static MD5Hash getMD5Hash(String key) throws IOException {
		return MD5Hash.digest(key.getBytes());
	}

	public static MD5Hash getMD5Hash(LongWritable key) throws IOException {
		return MD5Hash.digest(key.toString());
	}

	public static MD5Hash getMD5Hash(BytesWritable key) throws IOException {
		return MD5Hash.digest(key.getBytes());
	}

	public static MD5Hash getMD5Hash(IntWritable key) throws IOException {
		return MD5Hash.digest(key.toString());
	}

	public static MD5Hash getMD5Hash(ArrayWritable key) throws IOException {
		return MD5Hash.digest(key.toString());
	}

	public static MD5Hash getMD5Hash(BooleanWritable key) throws IOException {
		return MD5Hash.digest(key.toString());
	}

	public static MD5Hash getMD5Hash(FloatWritable key) throws IOException {
		return MD5Hash.digest(key.toString());
	}

	public static MD5Hash getMD5Hash(ByteWritable key) throws IOException {
		return MD5Hash.digest(key.toString());
	}

	public static MD5Hash getMD5Hash(DoubleWritable key) throws IOException {
		return MD5Hash.digest(key.toString());
	}

}
