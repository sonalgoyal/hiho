package co.nubetech.hiho.mapred.avro;

import java.io.IOException;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class GenericRecordInputFormat<T> extends AvroInputFormat<T>{
	  @Override
	  public RecordReader<AvroWrapper<T>, NullWritable>
	    getRecordReader(InputSplit split, JobConf job, Reporter reporter)
	    throws IOException {
	    reporter.setStatus(split.toString());
	    return new GenericRecordReader<T>(job, (FileSplit)split);
	  }

}
