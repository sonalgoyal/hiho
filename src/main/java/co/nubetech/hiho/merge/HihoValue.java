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
package co.nubetech.hiho.merge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HihoValue<V extends Writable> implements Writable {

	protected BooleanWritable isOld;
	protected V val;
	private Text valClass;

	/*@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((isOld == null) ? 0 : isOld.hashCode());
		result = prime * result + ((val == null) ? 0 : val.hashCode());
		result = prime * result
				+ ((valClass == null) ? 0 : valClass.hashCode());
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
		HihoValue other = (HihoValue) obj;
		if (isOld == null) {
			if (other.isOld != null)
				return false;
		} else if (!isOld.equals(other.isOld))
			return false;
		if (val == null) {
			if (other.val != null)
				return false;
		} else if (!val.equals(other.val))
			return false;
		if (valClass == null) {
			if (other.valClass != null)
				return false;
		} else if (!valClass.equals(other.valClass))
			return false;
		return true;
	}

	public V getVal() {
		return val;
	}

	public Boolean getIsOld() {
		return isOld.get();
	}

	public void setVal(V val) {
		this.val = val;
		this.valClass = new Text(val.getClass().getName());
	}

	public void setIsOld(Boolean isOld) {
		this.isOld = new BooleanWritable(isOld);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		isOld = new BooleanWritable();
		isOld.readFields(in);
		valClass = new Text();
		valClass.readFields(in);
		try {
			val = (V) Class.forName(valClass.toString()).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		val.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		isOld.write(out);
		valClass.write(out);
		val.write(out);
	}

}
