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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

public class DelimitedLineRecordReader extends RecordReader<Text, Text> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.DelimitedLineRecordReader.class);

	private String delimiter;
	private int column;
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private Text key = null;
	private Text value = null;

	/**
	 * 
	 * @param delimiter
	 * @param column
	 * 
	 * 
	 */

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.delimiter = job.get(DelimitedTextInputFormat.DELIMITER_CONF);
		this.column = job.getInt(DelimitedTextInputFormat.COLUMN_CONF, 0);
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			in = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	public boolean nextKeyValue() throws IOException {

		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		while (pos < end) {
			newSize = in.readLine(value, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			logger.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			// we calculate the key from the value here
			if (value != null) {
				logger.debug("Value is: " + value);
				logger.debug("Column is: " + column);
				logger.debug("Delimiter is: " + delimiter);

				key = getColumn(value, column, delimiter);

				logger.debug("Value after generating keyColumn: " + value);
				logger.debug("Key is: " + key);

			}
			return true;
		}
	}

	@Override
	public Text getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public Text getColumn(Text val, int column, String delimiter)
			throws IOException {
		if (delimiter == null || delimiter.equals("")) {
			throw new IOException("Value of delimiter is empty");
		}
		int lastOccurance = 0;
		int occurance = 0;
		for (int i = 0; i < column; i++) {
			occurance = val.find(delimiter, lastOccurance) - lastOccurance;
			lastOccurance = lastOccurance + occurance + delimiter.length();
		}

		logger.debug("text value is: " + val);
		int delimiterLength = delimiter.length();
		int startPosition = lastOccurance - (occurance + delimiterLength);
		Text keyColumn = new Text();
		keyColumn.set(val.getBytes(), startPosition, occurance);
		return keyColumn;
	}

}
