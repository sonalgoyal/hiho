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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class HihoTuple<K extends Writable> implements
		WritableComparable<HihoTuple> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.HihoTuple.class);

	protected MD5Hash hash;
	protected K key;
	private Text keyClass;

	public K getKey() {
		return key;
	}

	public void setKey(K key) throws IOException {
		this.key = key;
		this.hash = HashUtility.getMD5Hash(key);
		this.keyClass = new Text(key.getClass().getName());
		logger.debug("Key is: " + this.getKey());
		logger.debug("Hash is: " + this.getHash());
	}

	public MD5Hash getHash() {
		return hash;
	}

	public void setHash(MD5Hash hash) {
		this.hash = hash;
	}

	@Override
	public int hashCode() {
		return getHash().hashCode();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		logger.debug("Reading fields");
		hash = new MD5Hash();
		hash.readFields(in);
		keyClass = new Text();
		keyClass.readFields(in);
		try {
			logger.debug("Key class in readField() of HihoTuple class is :"
					+ keyClass);
			key = (K) Class.forName(keyClass.toString()).newInstance();

		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Error in serializing the HihoTuple ", e);
		}
		key.readFields(in);
	}

	/*@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hash == null) ? 0 : hash.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result
				+ ((keyClass == null) ? 0 : keyClass.hashCode());
		return result;
	}*/

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HihoTuple other = (HihoTuple) obj;
		if (hash == null) {
			if (other.hash != null)
				return false;
		} else if (!hash.equals(other.hash))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (keyClass == null) {
			if (other.keyClass != null)
				return false;
		} else if (!keyClass.equals(other.keyClass))
			return false;
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		logger.debug("Writing fields");
		hash.write(out);
		keyClass.write(out);
		key.write(out);

	}

	@Override
	public int compareTo(HihoTuple hihoTuple) {
		return WritableComparator.compareBytes(this.hash.getDigest(), 0,
				this.hash.MD5_LEN, hihoTuple.hash.getDigest(), 0,
				hihoTuple.hash.MD5_LEN);
	}

}
