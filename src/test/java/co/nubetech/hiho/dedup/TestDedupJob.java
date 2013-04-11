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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.HihoTestCase;
import co.nubetech.hiho.testdata.Student;

public class TestDedupJob extends HihoTestCase {
	final static Logger logger = Logger
	.getLogger(co.nubetech.hiho.dedup.TestDedupJob.class);
	
	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] {
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key",
				"-inputKeyClassName","org.apache.hadoop.io.IntWritable", 
				"-inputValueClassName",	"org.apache.hadoop.io.Text",
				"-inputPath", "testData/dedup/inputForSeqTest",
				"-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(args);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputFormat()
			throws HIHOException {
		String[] arg = new String[] { 
				"-dedupBy", "key",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName",	"org.apache.hadoop.io.Text", 
				"-inputPath", "testData/dedup/inputForSeqTest", 
				"-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInDedupBy()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-inputPath", "testData/dedup/inputForSeqTest",
				"-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputKeyClassName()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key",
				"-inputValueClassName",	"org.apache.hadoop.io.Text", 
				"-inputPath", "testData/dedup/inputForSeqTest",
				"-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputValueClassName()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key",
				"-inputKeyClassName","org.apache.hadoop.io.IntWritable",
				"-inputPath", "testData/dedup/inputForSeqTest",
				"-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputPath()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable", 
				"-inputValueClassName",	"org.apache.hadoop.io.Text", 
				"-outputPath", "output" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInOutputPath()
			throws HIHOException {
		String[] arg = new String[] {
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-dedupBy", "key",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable", 
				"-inputValueClassName",	"org.apache.hadoop.io.Text",
				"-inputPath", "testData/dedup/inputForSeqTest" };
		DedupJob dedupJob = new DedupJob();
		dedupJob.populateConfiguration(arg);
		dedupJob.checkMandatoryConfs();
	}
	
	
	public DedupJob runDedupJob(String[] args) throws Exception{
		DedupJob job = new DedupJob();			
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);
		return job;
	}	
	
	
	@Test
	public void testDedupByValueWithDelimitedTextInputFormat() throws Exception {
		final String inputData1 = "Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney\n" + 
		"Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein\n" + 
		"Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson";
		final String inputData2 = "Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos\n" + 
		"Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein\n" + 
		"Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson";
		createTextFileInHDFS(inputData1, "/input1", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input2", "testFile2.txt");	
		String[] args = new String[] {
				"-inputFormat", "co.nubetech.hiho.dedup.DelimitedTextInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.Text",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-inputPath", "/input1,/input2",
				"-outputPath", "output",
				"-delimeter", ",",
				"-column", "1",
				"-dedupBy", "value" };
		DedupJob job = runDedupJob(args);	
		assertEquals(6, job.getTotalRecordsRead());
		assertEquals(0, job.getBadRecords());
		assertEquals(5, job.getOutput());
		assertEquals(1, job.getDuplicateRecords());
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney");
		expectedOutput.add("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos");
		expectedOutput.add("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein");
		expectedOutput.add("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson");
		expectedOutput.add("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() );
			FSDataInputStream in = outputFS.open(fileStat.getPath());
			String line = null;			
			while ((line = in.readLine()) != null) {
				logger.debug("Output is " + line);
				assertTrue("Matched output " + line , expectedOutput.contains(line));
				expectedOutput.remove(line);
				count++;
			}
			in.close();
		}
		assertEquals(5, count);	
	}
	
	
	@Test
	public void testDedupByValueWithTextInputFormat() throws Exception {
		final String inputData1 = "Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney\n" + 
		"Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein\n" + 
		"Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson";
		final String inputData2 = "Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos\n" + 
		"Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein\n" + 
		"Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson";
		createTextFileInHDFS(inputData1, "/input1", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input2", "testFile2.txt");	
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
				"-inputPath", "/input1,/input2",
				"-outputPath", "output",
				"-outputFormat", "co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat",
				"-dedupBy", "value" };	
		DedupJob job = runDedupJob(args);	
		assertEquals(6, job.getTotalRecordsRead());
		assertEquals(0, job.getBadRecords());
		assertEquals(5, job.getOutput());
		assertEquals(1, job.getDuplicateRecords());
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney");
		expectedOutput.add("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos");
		expectedOutput.add("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein");
		expectedOutput.add("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson");
		expectedOutput.add("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() );
			FSDataInputStream in = outputFS.open(fileStat.getPath());
			String line = null;			
			while ((line = in.readLine()) != null) {
				logger.debug("Output is " + line);
				assertTrue("Matched output " + line , expectedOutput.contains(line));
				expectedOutput.remove(line);
				count++;
			}
			in.close();
		}
		assertEquals(5, count);	
	}
	
	@Test
	public void testDedupByIntWritableKeyWithSequenceFileInputFormat() throws Exception {
		HashMap<IntWritable, Text> inputData1 = new HashMap<IntWritable, Text>();
		inputData1.put(new IntWritable(1), new Text("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney"));
		inputData1.put(new IntWritable(2), new Text("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson"));
		inputData1.put(new IntWritable(3), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<IntWritable, Text> inputData2 = new HashMap<IntWritable, Text>();
		inputData2.put(new IntWritable(1), new Text("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos"));
		inputData2.put(new IntWritable(2), new Text("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson"));
		inputData2.put(new IntWritable(4), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
		
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputPath", "/input1,/input2",
				"-outputPath", "output",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-dedupBy", "key" };
		DedupJob job = runDedupJob(args);	
		assertEquals(6, job.getTotalRecordsRead());
		assertEquals(0, job.getBadRecords());
		assertEquals(4, job.getOutput());
		assertEquals(2, job.getDuplicateRecords());
		
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output/part-r-00000");
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
		Writable writableKey = (Writable)
		ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable writableValue = (Writable)
		ReflectionUtils.newInstance(reader.getValueClass(), conf);
		List<IntWritable> expectedOutput = new ArrayList<IntWritable>();
		expectedOutput.add(new IntWritable(1));
		expectedOutput.add(new IntWritable(2));
		expectedOutput.add(new IntWritable(3));
		expectedOutput.add(new IntWritable(4));
		int count = 0;
		while (reader.next(writableKey, writableValue)) {
			logger.debug("key and value is: " + writableKey + ", " + writableValue);
			assertTrue("Matched output " + writableKey , expectedOutput.contains(writableKey));
			count++;
		}
		IOUtils.closeStream(reader);
		assertEquals(4, count);	
	} 
	
	@Test
	public void testDedupByValueWithSequenceFileInputFormat() throws Exception {
		HashMap<IntWritable, Text> inputData1 = new HashMap<IntWritable, Text>();
		inputData1.put(new IntWritable(1), new Text("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney"));
		inputData1.put(new IntWritable(2), new Text("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson"));
		inputData1.put(new IntWritable(3), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<IntWritable, Text> inputData2 = new HashMap<IntWritable, Text>();
		inputData2.put(new IntWritable(1), new Text("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos"));
		inputData2.put(new IntWritable(2), new Text("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson"));
		inputData2.put(new IntWritable(4), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
		
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputPath", "/input1,/input2",
				"-outputPath", "output",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-dedupBy", "value" };
		DedupJob job = runDedupJob(args);	
		assertEquals(6, job.getTotalRecordsRead());
		assertEquals(0, job.getBadRecords());
		assertEquals(5, job.getOutput());
		assertEquals(1, job.getDuplicateRecords());
		
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output/part-r-00000");
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
		Writable writableKey = (Writable)
		ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable writableValue = (Writable)
		ReflectionUtils.newInstance(reader.getValueClass(), conf);
		List<Text> expectedOutput = new ArrayList<Text>();
		expectedOutput.add(new Text("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney"));
		expectedOutput.add(new Text("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson"));
		expectedOutput.add(new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		expectedOutput.add(new Text("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos"));
		expectedOutput.add(new Text("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson"));
		int count = 0;
		while (reader.next(writableKey, writableValue)) {
			logger.debug("key and value is: " + writableKey + ", " + writableValue);
			assertTrue("Matched output " + writableValue , expectedOutput.contains(writableValue));
			count++;
		}
		IOUtils.closeStream(reader);
		assertEquals(5, count);	
	}
	
	@Test
	public void testDedupByLongWritableKeyWithSequenceFileInputFormat() throws Exception {
		HashMap<LongWritable, Text> inputData1 = new HashMap<LongWritable, Text>();
		inputData1.put(new LongWritable(1), new Text("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney"));
		inputData1.put(new LongWritable(2), new Text("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson"));
		inputData1.put(new LongWritable(3), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<LongWritable, Text> inputData2 = new HashMap<LongWritable, Text>();
		inputData2.put(new LongWritable(1), new Text("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos"));
		inputData2.put(new LongWritable(2), new Text("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson"));
		inputData2.put(new LongWritable(4), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
		
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputPath", "/input1,/input2",
				"-outputPath", "output",
				"-inputKeyClassName", "org.apache.hadoop.io.LongWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-dedupBy", "key" };
		DedupJob job = runDedupJob(args);	
		assertEquals(6, job.getTotalRecordsRead());
		assertEquals(0, job.getBadRecords());
		assertEquals(4, job.getOutput());
		assertEquals(2, job.getDuplicateRecords());
		
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output/part-r-00000");
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
		Writable writableKey = (Writable)
		ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable writableValue = (Writable)
		ReflectionUtils.newInstance(reader.getValueClass(), conf);
		List<LongWritable> expectedOutput = new ArrayList<LongWritable>();
		expectedOutput.add(new LongWritable(1));
		expectedOutput.add(new LongWritable(2));
		expectedOutput.add(new LongWritable(3));
		expectedOutput.add(new LongWritable(4));
		int count = 0;
		while (reader.next(writableKey, writableValue)) {
			logger.debug("key and value is: " + writableKey + ", " + writableValue);
			assertTrue("Matched output " + writableKey , expectedOutput.contains(writableKey));
			count++;
		}
		IOUtils.closeStream(reader);
		assertEquals(4, count);
	}	
	
	
	@Test
	public void testDedupByValueWithSequenceFileAsTextInputFormat() throws Exception {
		HashMap<Text, Text> inputData1 = new HashMap<Text, Text>();
		inputData1.put(new Text("1"), new Text("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney"));
		inputData1.put(new Text("2"), new Text("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson"));
		inputData1.put(new Text("3"), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<Text, Text> inputData2 = new HashMap<Text, Text>();
		inputData2.put(new Text("1"), new Text("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos"));
		inputData2.put(new Text("2"), new Text("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson"));
		inputData2.put(new Text("4"), new Text("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
		
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat",
				"-outputFormat", "co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat",
				"-inputPath", "/input1,/input2",
				"-outputPath", "output",
				"-inputKeyClassName", "org.apache.hadoop.io.Text",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-dedupBy", "value" };
		DedupJob job = runDedupJob(args);	
		assertEquals(6, job.getTotalRecordsRead());
		assertEquals(0, job.getBadRecords());
		assertEquals(5, job.getOutput());
		assertEquals(1, job.getDuplicateRecords());
		
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Xavier Wilson,Mason Holloway,Carlos Johnston,Martin Noel,Drake Mckinney");
		expectedOutput.add("Kennedy Bailey,Jerome Perry,David Cabrera,Edan Fleming,Orlando Tyson");
		expectedOutput.add("Drake Mckinney,Murphy Baird,Theodore Lindsey,Nehru Wilcox,Harper Klein");
		expectedOutput.add("Zephania Bauer,Jermaine Gordon,Vincent Moon,Steven Pierce,Jasper Campos");
		expectedOutput.add("Kennedy Bailey,Plato Atkinson,Stuart Guy,Rooney Levy,Judah Benson");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() );
			FSDataInputStream in = outputFS.open(fileStat.getPath());
			String line = null;			
			while ((line = in.readLine()) != null) {
				logger.debug("Output is " + line);
				assertTrue("Matched output " + line , expectedOutput.contains(line));
				expectedOutput.remove(line);
				count++;
			}
			in.close();
		}
		assertEquals(5, count);	
	}
	
	
	@Test
	public void testDedupByCustomObjectKeyWithSequenceFileInputFormat() throws Exception {		
		Student student1 = setStudent(new Text("Sam"),new Text("US"),new IntWritable(1),
				new LongWritable(9999999998l),new DoubleWritable(99.12));				
		Student student2 = setStudent(new Text("John"),new Text("AUS"),new IntWritable(2),
				new LongWritable(9999999999l),new DoubleWritable(90.12));				
		Student student3 = setStudent(new Text("Mary"),new Text("UK"),new IntWritable(3),
				new LongWritable(9999999988l),new DoubleWritable(69.12));		
		Student student4 = setStudent(new Text("Kelvin"),new Text("UK"),new IntWritable(4),
				new LongWritable(9999998888l),new DoubleWritable(59.12));
	
		HashMap<Student, Text> inputData1 = new HashMap<Student, Text>();
		inputData1.put(student1, new Text("Macon Kent,6269 Aenean St.,1-247-399-1051,08253"));
		inputData1.put(student2, new Text("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"));
		inputData1.put(student3, new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<Student, Text> inputData2 = new HashMap<Student, Text>();
		inputData2.put(student2, new Text("Austin Farley,4794 Donec Ave,1-230-823-8164,13508"));
		inputData2.put(student3, new Text("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"));
		inputData2.put(student4, new Text("Timon Leonard,716 Ac Ave,1-857-935-3882,62240"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
	

		String[] args = new String[] {
				"-inputPath", "/input1,/input2",
				"-outputPath", "output",
				"-dedupBy", "key",
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "co.nubetech.hiho.testdata.Student",
				"-inputValueClassName", "org.apache.hadoop.io.Text" };
		DedupJob job=runDedupJob(args);
		assertEquals(6, job.getTotalRecordsRead());
		assertEquals(0, job.getBadRecords());
		assertEquals(4, job.getOutput());
		assertEquals(2, job.getDuplicateRecords());
		
	    FileSystem outputFS = getFileSystem();
	    Path outputPath = new Path(outputFS.getHomeDirectory(), "output/part-r-00000");
	    Configuration conf = new Configuration();
	    SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
	    Writable writableKey = (Writable)
	    ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    Writable writableValue = (Writable)
	    ReflectionUtils.newInstance(reader.getValueClass(), conf);
	    List<Student> expectedOutput = new ArrayList<Student>();
	    expectedOutput.add(student1);
	    expectedOutput.add(student2);
	    expectedOutput.add(student3);
	    expectedOutput.add(student4);
	    int count = 0;
	    while (reader.next(writableKey, writableValue)) {
	    	logger.debug("key and value is: " + writableKey + ", " + writableValue);
	    	assertTrue("Matched output " + writableKey , expectedOutput.contains(writableKey));
	    	count++;
	    }
	    IOUtils.closeStream(reader);
	    assertEquals(4, count);  
	} 
	
	
	
	public Student setStudent(Text name, Text address, IntWritable id, 
			LongWritable mobNumber, DoubleWritable per){
		Student student = new Student();
		student.setName(name);
		student.setAddress(address);
		student.setId(id);
		student.setMobileNumber(mobNumber);
		student.setPercentage(per);
		return student;
	}
	
	
}
