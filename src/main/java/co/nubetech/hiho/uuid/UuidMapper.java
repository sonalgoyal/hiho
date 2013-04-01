package co.nubetech.hiho.uuid;

import static co.nubetech.hiho.dedup.DelimitedTextInputFormat.DELIMITER_CONF;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;

public class UuidMapper extends Mapper<Text, Text, NullWritable, Text> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.uuid.UuidMapper.class);

	@Override
	public void map(Text key, Text val, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		UUID randomUuid = UUID.randomUUID();
		TaskAttemptID taskAttemptId = context.getTaskAttemptID();
		Long mostSignificant = (long) taskAttemptId.hashCode();
		Long leastSignificant = randomUuid.getLeastSignificantBits();
		UUID uniquieId = new UUID(mostSignificant, leastSignificant);

		String delimeter = conf.get(DELIMITER_CONF);
		String value = val.toString();
		String outputValue = uniquieId.toString() + delimeter + value;
		logger.debug("The UUID generated is " + uniquieId.toString()
				+ "\nOutput is " + outputValue);

		context.write(null, new Text(outputValue));

	}

}
