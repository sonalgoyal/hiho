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
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import co.nubetech.hiho.common.HIHOConf;

public class FTPTextOutputFormat<K, V> extends TextOutputFormat<K, V> {

	protected static class FTPLineRecordWriter<K, V> extends
			LineRecordWriter<K, V> {
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

		private final byte[] keyValueSeparator;
		private FTPClient ftpClient;

		public FTPLineRecordWriter(DataOutputStream out,
				String keyValueSeparator, FTPClient ftpClient) {
			super(out, keyValueSeparator);
			this.ftpClient = ftpClient;
			try {
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		public FTPLineRecordWriter(DataOutputStream out, FTPClient ftpClient) {
			this(out, "\t", ftpClient);
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

		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			out.close();
			ftpClient.disconnect();
		}
	}

	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {

		Configuration conf = job.getConfiguration();

		String ip = conf.get(HIHOConf.FTP_ADDRESS);
		String portno = conf.get(HIHOConf.FTP_PORT);
		String usr = conf.get(HIHOConf.FTP_USER);
		String pwd = conf.get(HIHOConf.FTP_PASSWORD);
		String dir = getOutputPath(job).toString();
		System.out.println("\n\ninside ftpoutputformat" + ip + " " + portno
				+ " " + usr + " " + pwd + " " + dir);
		String keyValueSeparator = conf.get(
				"mapred.textoutputformat.separator", "\t");
		FTPClient f = new FTPClient();
		f.connect(ip, Integer.parseInt(portno));
		f.login(usr, pwd);
		f.changeWorkingDirectory(dir);
		f.setFileType(FTP.BINARY_FILE_TYPE);

		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		String filename = file.getName();
		if (!isCompressed) {
			// FSDataOutputStream fileOut = fs.create(file, false);
			OutputStream os = f.appendFileStream(filename);
			DataOutputStream fileOut = new DataOutputStream(os);
			return new FTPLineRecordWriter<K, V>(fileOut, new String(
					keyValueSeparator), f);

		} else {
			// FSDataOutputStream fileOut = fs.create(file, false);
			OutputStream os = f.appendFileStream(filename);
			DataOutputStream fileOut = new DataOutputStream(os);
			return new FTPLineRecordWriter<K, V>(new DataOutputStream(
					codec.createOutputStream(fileOut)), keyValueSeparator, f);
		}
	}

}
