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

package co.nubetech.hiho.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class FileStreamRecordReader extends
		RecordReader<Text, FSDataInputStream> {
	private FileSplit split;
	private TaskAttemptContext context;
	private FSDataInputStream stream;
	private boolean isRead = false;
	private String fileName;

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.input.FileStreamRecordReader.class);

	public FileStreamRecordReader(FileSplit split, TaskAttemptContext context) {
		this.split = split;
		this.context = context;
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		this.split = (FileSplit) genericSplit;
		this.context = context;

	}

	@Override
	public void close() throws IOException {
		if (stream != null) {
			IOUtils.closeStream(stream);
		}
	}

	@Override
	public Text getCurrentKey() {
		return new Text(fileName);
	}

	@Override
	public FSDataInputStream getCurrentValue() {
		logger.debug("Returning value ");
		return stream;
	}

	@Override
	public float getProgress() throws IOException {
		return 0f;
		// dummy
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		logger.debug("Inside nextKeyValue");
		if (!isRead) {
			Path file = split.getPath();
			logger.debug("Path is " + file);
			fileName = file.getName();
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			stream = fs.open(file);
			logger.debug("Opened stream");
			isRead = true;
			return true;
		}
		return false;
	}

}
