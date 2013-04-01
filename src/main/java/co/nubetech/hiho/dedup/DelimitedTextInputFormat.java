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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

public class DelimitedTextInputFormat extends FileInputFormat<Text, Text> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.dedup.DelimitedTextInputFormat.class);

	public final static String DELIMITER_CONF = "mapreduce.input.hiho.delimited.delimiter";
	public final static String COLUMN_CONF = "mapreduce.input.hiho.delimited.column";

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) {
		logger.debug("Delimiter is: "
				+ context.getConfiguration().get(DELIMITER_CONF));
		logger.debug("Column is: "
				+ context.getConfiguration().getInt(COLUMN_CONF, 0));
		return new DelimitedLineRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	public static void setProperties(Job job, String delim, int column) {
		job.getConfiguration().set(DELIMITER_CONF, delim);
		job.getConfiguration().setInt(COLUMN_CONF, column);
	}
}
