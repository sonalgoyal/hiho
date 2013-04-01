/**
 * Copyright 2010 Nube Technologies
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

package co.nubetech.hiho.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class ignores the key and simply prints the value. However, new line is
 * added
 * 
 * @author sgoyal
 * 
 */
public class NoKeyOnlyValueOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class NoKeyRecordWriter<K, V> extends RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";

		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		protected DataOutputStream out;

		public NoKeyRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(K key, V value) throws IOException {
			writeObject(value);
			out.write(newline);
		}

		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			out.close();
		}
	}

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException {
		boolean isCompressed = getCompressOutput(context);
		Configuration conf = context.getConfiguration();
		String ext = "";
		CompressionCodec codec = null;

		if (isCompressed) {
			// create the named codec
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					context, GzipCodec.class);
			codec = ReflectionUtils.newInstance(codecClass, conf);

			ext = codec.getDefaultExtension();
		}

		Path file = getDefaultWorkFile(context, ext);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		DataOutputStream ostream = fileOut;

		if (isCompressed) {
			ostream = new DataOutputStream(codec.createOutputStream(fileOut));
		}

		return new NoKeyRecordWriter<K, V>(ostream);
	}
}
