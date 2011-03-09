package co.nubetech.hiho.testdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Student implements WritableComparable {
	private IntWritable id;
	private Text name;
	private Text address;
	private LongWritable mobileNumber;
	private DoubleWritable percentage;

	public void setId(IntWritable id) {
		this.id = id;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public void setAddress(Text address) {
		this.address = address;
	}

	public void setMobileNumber(LongWritable number) {
		mobileNumber = number;
	}

	public void setPercentage(DoubleWritable per) {
		percentage = per;
	}

	public IntWritable getId() {
		return id;
	}

	public Text getName() {
		return name;
	}

	public Text getAddress() {
		return address;
	}

	public LongWritable getMobileNumber() {
		return mobileNumber;
	}

	public DoubleWritable getPercentage() {
		return percentage;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = new IntWritable();
		id.readFields(in);
		name = new Text();
		name.readFields(in);
		address = new Text();
		address.readFields(in);
		mobileNumber = new LongWritable();
		mobileNumber.readFields(in);
		percentage = new DoubleWritable();
		percentage.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		name.write(out);
		address.write(out);
		mobileNumber.write(out);
		percentage.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result
				+ ((mobileNumber == null) ? 0 : mobileNumber.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((percentage == null) ? 0 : percentage.hashCode());
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
		Student other = (Student) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (mobileNumber == null) {
			if (other.mobileNumber != null)
				return false;
		} else if (!mobileNumber.equals(other.mobileNumber))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (percentage == null) {
			if (other.percentage != null)
				return false;
		} else if (!percentage.equals(other.percentage))
			return false;
		return true;
	}

	@Override
	public int compareTo(Object arg0) {
		Text thatName= (Text) arg0;
		return this.name.compareTo(thatName);
	}

}
