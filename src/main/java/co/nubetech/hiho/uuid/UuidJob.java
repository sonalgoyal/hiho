package co.nubetech.hiho.uuid;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOConf;
import co.nubetech.hiho.dedup.DelimitedTextInputFormat;
import co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat;

public class UuidJob extends Configured implements Tool {

	private final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.uuid.UuidJob.class);

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("UUID_Job");
		job.setMapperClass(UuidMapper.class);
		job.setJarByClass(UuidJob.class);
		for (Entry<String, String> entry : conf) {
			logger.debug("key, value " + entry.getKey() + "="
					+ entry.getValue());
		}
		job.getConfiguration().setInt(HIHOConf.NUMBER_MAPPERS, 5);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(DelimitedTextInputFormat.class);
		DelimitedTextInputFormat.addInputPath(job, new Path(args[0]));
		DelimitedTextInputFormat.setProperties(job, ",", 1);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(NoKeyOnlyValueOutputFormat.class);
		NoKeyOnlyValueOutputFormat.setOutputPath(job, new Path("output"));
		int ret = 0;
		try {
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UuidJob(), args);
		System.exit(res);
	}

}
