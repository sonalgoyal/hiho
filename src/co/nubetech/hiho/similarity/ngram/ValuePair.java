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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ValuePair implements WritableComparable {

	protected Text value1;
	protected Text value2;

	@Override
	public String toString() {
		return "ValuePair [value1=" + value1 + ", value2=" + value2 + "]";
	}

	public Text getValue1() {
		return value1;
	}

	public void setValue1(Text value1) {
		this.value1 = value1;
	}

	public Text getValue2() {
		return value2;
	}

	public void setValue2(Text value2) {
		this.value2 = value2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value1 = new Text();
		value1.readFields(in);

		value2 = new Text();
		value2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		value1.write(out);
		value2.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value1 == null) ? 0 : value1.hashCode());
		result = prime * result + ((value2 == null) ? 0 : value2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ValuePair other = (ValuePair) obj;
		if (value1 == null) {
			if (other.value1 != null)
				return false;
		} else if (!value1.equals(other.value1))
			return false;
		if (value2 == null) {
			if (other.value2 != null)
				return false;
		} else if (!value2.equals(other.value2))
			return false;
		return true;
	}

	@Override
	public int compareTo(Object o) {
		ValuePair valPair = (ValuePair) o;
		int compareValue1 = this.getValue1().compareTo(valPair.getValue1());
		int compareValue2 = this.getValue2().compareTo(valPair.getValue2());
		if (compareValue1 == compareValue2) {
			return compareValue1;
		} else if (compareValue1 == 0) {
			return compareValue2;
		} else {
			return compareValue1;
		}
	}

}