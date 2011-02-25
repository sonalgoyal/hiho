package co.nubetech.hiho.testdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Student implements Writable {
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

}
