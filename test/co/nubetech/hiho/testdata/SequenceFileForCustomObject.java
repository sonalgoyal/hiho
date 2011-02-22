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
package co.nubetech.hiho.testdata;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileForCustomObject {
	private static IntWritable id[] = { new IntWritable(100),
			new IntWritable(101), new IntWritable(102), new IntWritable(103),
			new IntWritable(104), new IntWritable(105), new IntWritable(106),
			new IntWritable(107), new IntWritable(108), new IntWritable(109) };
	private static Text name[] = { new Text("Asd"), new Text("Bcf"),
			new Text("Cefg"), new Text("D"), new Text("E"), new Text("F"),
			new Text("G"), new Text("H"), new Text("J"), new Text("K") };
	private static Text address[] = { new Text("abc"), new Text("asd"),
			new Text("qwe"), new Text("fgh"), new Text("bnv"), new Text("iou"),
			new Text("iwf"), new Text("ugm"), new Text("ytr"), new Text("pkl") };
	private static LongWritable mobileNo[] = { new LongWritable(123),
			new LongWritable(234), new LongWritable(345),
			new LongWritable(456), new LongWritable(567),
			new LongWritable(678), new LongWritable(789),
			new LongWritable(890), new LongWritable(901), new LongWritable(012) };
	private static DoubleWritable percentage[] = { new DoubleWritable(45.5),
			new DoubleWritable(67.4), new DoubleWritable(32.1),
			new DoubleWritable(89.32), new DoubleWritable(45.5),
			new DoubleWritable(67.5), new DoubleWritable(69.6),
			new DoubleWritable(65), new DoubleWritable(63.87),
			new DoubleWritable(77.36), new DoubleWritable(73.11) };

	public static void main(String[] args) throws IOException {
		String uri = "inputnew.seq";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		// IntWritable key = new IntWritable();
		// Student value[] = new Student[10];
		SequenceFile.Writer writer = null;
		Student student = new Student();
		try {
			writer = SequenceFile.createWriter(fs, conf, path,
					IntWritable.class, Student.class);
			for (int i = 0; i < 10; i++) {
				student.setId(id[i]);
				student.setName(name[i]);
				student.setAddress(address[i]);
				student.setMobileNumber(mobileNo[i]);
				student.setPercentage(percentage[i]);
				// value[i]=student;

				// System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key,
				// value);
				writer.append(student.getId(), student);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}

	}
}
