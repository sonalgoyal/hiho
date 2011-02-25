package co.nubetech.hiho.mapred.avro;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class GenericRecordAvroOutputFormat<T> extends AvroOutputFormat<T>{
	
	public RecordWriter<AvroWrapper<T>, NullWritable>
    getRecordWriter(FileSystem ignore, JobConf job,
                    String name, Progressable prog)
    throws IOException {

    boolean isMapOnly = job.getNumReduceTasks() == 0;
    Schema schema = isMapOnly
      ? AvroJob.getMapOutputSchema(job)
      : AvroJob.getOutputSchema(job);

    final DataFileWriter<T> writer =
      new DataFileWriter<T>(new GenericDatumWriter<T>());

    if (FileOutputFormat.getCompressOutput(job)) {
      int level = job.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
      writer.setCodec(CodecFactory.deflateCodec(level));
    }

    // copy metadata from job
    for (Map.Entry<String,String> e : job) {
      if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.TEXT_PREFIX.length()),
                       e.getValue());
      if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.BINARY_PREFIX.length()),
                       URLDecoder.decode(e.getValue(), "ISO-8859-1")
                       .getBytes("ISO-8859-1"));
    }

    Path path = FileOutputFormat.getTaskOutputPath(job, name+EXT);
    writer.create(schema, path.getFileSystem(job).create(path));

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
        public void write(AvroWrapper<T> wrapper, NullWritable ignore)
          throws IOException {
          writer.append(wrapper.datum());
        }
        public void close(Reporter reporter) throws IOException {
          writer.close();
        }
      };
  }


	
}
