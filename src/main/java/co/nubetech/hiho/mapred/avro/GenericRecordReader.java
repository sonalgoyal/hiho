package co.nubetech.hiho.mapred.avro;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

public class GenericRecordReader<T> extends AvroRecordReader<T> {
	
	public GenericRecordReader(JobConf job, FileSplit split)
    throws IOException {
    super(new DataFileReader<T>
         (new FsInput(split.getPath(), job),
          new SpecificDatumReader<T>(AvroJob.getInputSchema(job))),
         split);
  }

}
