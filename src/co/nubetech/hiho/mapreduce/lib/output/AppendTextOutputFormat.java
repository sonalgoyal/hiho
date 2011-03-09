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

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import co.nubetech.hiho.common.HIHOConf;

public class AppendTextOutputFormat extends TextOutputFormat {
	private String isAppend;
	private static long fileCount;

	private static final NumberFormat NUMBER_FORMAT = NumberFormat
			.getInstance();

	static {
		NUMBER_FORMAT.setMinimumIntegerDigits(5);
		NUMBER_FORMAT.setGroupingUsed(false);
	}

	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
		try {
			super.checkOutputSpecs(job);
		} catch (FileAlreadyExistsException e) {

			Configuration conf = job.getConfiguration();

			isAppend = conf.get(HIHOConf.IS_APPEND, "false");
			if (isAppend.equalsIgnoreCase("false")) {
				throw new FileAlreadyExistsException();
			} else {
				Path outDir = getOutputPath(job);
				if (outDir == null) {
					throw new InvalidJobConfException(
							"OUTPUT directory not set.");
				}
			}
		}
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		Path p1;
		isAppend = context.getConfiguration().get(HIHOConf.IS_APPEND);
		if (isAppend.equalsIgnoreCase("false")) {
			p1 = super.getDefaultWorkFile(context, extension);
		} else {
			FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
			Path p = committer.getWorkPath();
			fileCount = p.getFileSystem(context.getConfiguration())
					.getContentSummary(getOutputPath(context)).getFileCount();
			if (fileCount > 1) {
				fileCount = fileCount - 1;
			}
			p1 = new Path(committer.getWorkPath(), getUniqueFile(context,
					"part", extension));
		}
		return p1;
	}

	public synchronized static String getUniqueFile(TaskAttemptContext context,
			String name, String extension) {

		TaskID taskId = context.getTaskAttemptID().getTaskID();
		int partition = taskId.getId();
		partition = partition + (int) fileCount;
		StringBuilder result = new StringBuilder();
		result.append(name);
		result.append('-');
		// result.append(taskId.isMap() ? 'm' : 'r');
		result.append('-');
		result.append(NUMBER_FORMAT.format(partition));
		result.append(extension);
		return result.toString();
	}

}
