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
package co.nubetech.hiho.merge;

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

public class TestMergeJob extends HihoTestCase {
	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.merge.TestMergeJob.class);

	@Test
	public void testCheckMandatoryConfsValidValues() throws HIHOException {
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-oldPath", "testData/merge/inputOld/old",
				"-newPath",	"testData/merge/inputNew/new", 
				"-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputFormat()
			throws HIHOException {
		String[] args = new String[] { 
				"-mergeBy", "key", 
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-oldPath", "testData/merge/inputForSeqTest/old",
				"-newPath", "testData/merge/inputForSeqTest/new",
				"-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInMergeBy()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-oldPath", "testData/merge/inputForSeqTest/old",
				"-newPath", "testData/merge/inputForSeqTest/new",
				"-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputKeyClassName()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-oldPath", "testData/merge/inputForSeqTest/old",
				"-newPath", "testData/merge/inputForSeqTest/new",
				"-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInInputValueClassName()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-oldPath", "testData/merge/inputForSeqTest/old",
				"-newPath", "testData/merge/inputForSeqTest/new",
				"-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInOldPath()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-newPath", "testData/merge/inputForSeqTest/new",
				"-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInNewPath()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key", 
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-oldPath",	"testData/merge/inputForSeqTest/old", 
				"-outputPath", "output" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
	}

	@Test(expected = HIHOException.class)
	public void testCheckMandatoryConfsForExpectedExceptionInOutputPath()
			throws HIHOException {
		String[] args = new String[] {
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-mergeBy", "key",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-oldPath", "testData/merge/inputForSeqTest/old",
				"-newPath", "testData/merge/inputForSeqTest/new" };
		MergeJob mergeJob = new MergeJob();
		mergeJob.populateConfiguration(args);
		mergeJob.checkMandatoryConfs();
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
	
	@Test
	public void testMergeByCustomObjectKeyWithSequenceFileInputFormat() throws Exception {		
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
				"-newPath",	"/input1",
				"-oldPath",	"/input2",
				"-mergeBy",	"key",
				"-outputPath", "output",
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "co.nubetech.hiho.testdata.Student",
				"-inputValueClassName", "org.apache.hadoop.io.Text" };
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(4,job.getOutput());
		
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

	
	@Test
	public void testMergeByIntWritableKeyWithSequenceFileInputFormat() throws Exception {
		HashMap<IntWritable, Text> inputData1 = new HashMap<IntWritable, Text>();
		inputData1.put(new IntWritable(1), new Text("Macon Kent,6269 Aenean St.,1-247-399-1051,08253"));
		inputData1.put(new IntWritable(2), new Text("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"));
		inputData1.put(new IntWritable(3), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<IntWritable, Text> inputData2 = new HashMap<IntWritable, Text>();
		inputData2.put(new IntWritable(1), new Text("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"));
		inputData2.put(new IntWritable(2), new Text("Timon Leonard,716 Ac Ave,1-857-935-3882,62240"));
		inputData2.put(new IntWritable(4), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
	

		String[] args = new String[] {
				"-newPath",	"/input1",
				"-oldPath",	"/input2",
				"-mergeBy",	"key",
				"-outputPath", "output",
				"-inputFormat", "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text" };
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(4,job.getOutput());
		
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
	public void testMergeByLongWritableKeyWithSequenceFileInputFormat() throws Exception {
		HashMap<LongWritable, Text> inputData1 = new HashMap<LongWritable, Text>();
		inputData1.put(new LongWritable(1), new Text("Macon Kent,6269 Aenean St.,1-247-399-1051,08253"));
		inputData1.put(new LongWritable(2), new Text("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"));
		inputData1.put(new LongWritable(3), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<LongWritable, Text> inputData2 = new HashMap<LongWritable, Text>();
		inputData2.put(new LongWritable(1), new Text("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"));
		inputData2.put(new LongWritable(2), new Text("Timon Leonard,716 Ac Ave,1-857-935-3882,62240"));
		inputData2.put(new LongWritable(4), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
	

		String[] args = new String[] {
				"-newPath",	"/input1",
				"-oldPath",	"/input2",
				"-mergeBy",	"key",
				"-outputPath","output",
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.LongWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text" };
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(4,job.getOutput());
		
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
	public void testMergeByValueWithSequenceFileInputFormat() throws Exception {
		HashMap<IntWritable, Text> inputData1 = new HashMap<IntWritable, Text>();
		inputData1.put(new IntWritable(1), new Text("Macon Kent,6269 Aenean St.,1-247-399-1051,08253"));
		inputData1.put(new IntWritable(2), new Text("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"));
		inputData1.put(new IntWritable(3), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<IntWritable, Text> inputData2 = new HashMap<IntWritable, Text>();
		inputData2.put(new IntWritable(1), new Text("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"));
		inputData2.put(new IntWritable(2), new Text("Timon Leonard,716 Ac Ave,1-857-935-3882,62240"));
		inputData2.put(new IntWritable(4), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");
		

		String[] args = new String[] {
				"-newPath",	"/input1",
				"-oldPath",	"/input2",
				"-mergeBy",	"value",
				"-outputPath","output",
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.IntWritable",
				"-inputValueClassName", "org.apache.hadoop.io.Text" };
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(5,job.getOutput());
		
	    FileSystem outputFS = getFileSystem();
        Path outputPath = new Path(outputFS.getHomeDirectory(), "output/part-r-00000");
        Configuration conf = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(outputFS, outputPath, conf);
        Writable writableKey = (Writable)
        ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable writableValue = (Writable)
        ReflectionUtils.newInstance(reader.getValueClass(), conf);
        List<Text> expectedOutput = new ArrayList<Text>();
        expectedOutput.add(new Text("Macon Kent,6269 Aenean St.,1-247-399-1051,08253"));
        expectedOutput.add(new Text("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"));
        expectedOutput.add(new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
        expectedOutput.add(new Text("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"));
        expectedOutput.add(new Text("Timon Leonard,716 Ac Ave,1-857-935-3882,62240"));
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
	public void testMergeByKeyWithDelimitedTextInputFormat() throws Exception {
		
		final String inputData1 = "Macon Kent,6269 Aenean St.,1-247-399-1051,08253"
				+"\nDale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"
				+"\nCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		final String inputData2 = "Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"
			    +"\nMacaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"
			    +"\nCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		createTextFileInHDFS(inputData1, "/input1", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input2", "testFile2.txt");
		
		String[] args = new String[] {
				"-newPath", "/input1",
				"-oldPath",	"/input2", 
				"-mergeBy", "key", 
				"-outputPath", "output",
				"-inputFormat",	"co.nubetech.hiho.dedup.DelimitedTextInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.Text",
				"-inputValueClassName", "org.apache.hadoop.io.Text" };
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(4,job.getOutput());
		
		FileSystem outputFS=getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Macon Kent,6269 Aenean St.,1-247-399-1051,08253");
		expectedOutput.add("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510");
		expectedOutput.add("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714");
		expectedOutput.add("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() + " and is it a dir? " + fileStat.isDirectory());
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
		assertEquals(4, count);
	
	}

	@Test
	public void testMergeByValueWithDelimitedTextInputFormat() throws Exception {
		
		final String inputData1 = "Macon Kent,6269 Aenean St.,1-247-399-1051,08253" +
				"\nDale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510" +
				"\nCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		final String inputData2 = "Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584" +
				"\nCharles Wood,525-9709 In Rd.,1-370-528-4758,62714" +
				"\nTimon Leonard,716 Ac Ave,1-857-935-3882,62240";
		createTextFileInHDFS(inputData1, "/input1", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input2", "testFile2.txt");
						
		String[] args = new String[] {
				"-newPath", "/input1",
				"-oldPath",	"/input2",
				"-mergeBy", "value", 
				"-outputPath", "output",
				"-inputFormat",	"co.nubetech.hiho.dedup.DelimitedTextInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.Text",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				};
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(5,job.getOutput());
		
		FileSystem outputFS=getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Macon Kent,6269 Aenean St.,1-247-399-1051,08253");
		expectedOutput.add("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510");
		expectedOutput.add("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714");
		expectedOutput.add("Timon Leonard,716 Ac Ave,1-857-935-3882,62240");
		expectedOutput.add("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() + " and is it a dir? " + fileStat.isDirectory());
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
	public void testMergeByValueWithTextInputFormat() throws Exception {
		
		final String inputData1 = "Macon Kent,6269 Aenean St.,1-247-399-1051,08253" +
				"\nDale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510" +
				"\nCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		final String inputData2 = "Timon Leonard,716 Ac Ave,1-857-935-3882,62240" +
				"\nMacaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584" +
				"\nCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		createTextFileInHDFS(inputData1, "/input1", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input2", "testFile2.txt");
		
		String[] args = new String[] {
				"-newPath", "/input1",
				"-oldPath",	"/input2",
				"-mergeBy", "value", 
				"-outputPath", "output",
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
				"-outputFormat", "co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat"
				};
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(5,job.getOutput());
		
		FileSystem outputFS=getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Macon Kent,6269 Aenean St.,1-247-399-1051,08253");
		expectedOutput.add("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510");
		expectedOutput.add("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714");
		expectedOutput.add("Timon Leonard,716 Ac Ave,1-857-935-3882,62240");
		expectedOutput.add("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() + " and is it a dir? " + fileStat.isDirectory());
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
	public void testMergeByKeyWithKeyValueTextInputFormat() throws Exception {
		
		final String inputData1 = "A\tMacon Kent,6269 Aenean St.,1-247-399-1051,08253" +
				"\nB\tDale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510" +
				"\nC\tCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		final String inputData2 = "A\tTimon Leonard,716 Ac Ave,1-857-935-3882,62240" +
				"\nD\tMacaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584" +
				"\nB\tCharles Wood,525-9709 In Rd.,1-370-528-4758,62714";
		createTextFileInHDFS(inputData1, "/input1", "testFile1.txt");	
		createTextFileInHDFS(inputData2, "/input2", "testFile2.txt");
		
		String[] args = new String[] {
				"-newPath", "/input1",
				"-oldPath",	"/input2",
				"-mergeBy", "key", 
				"-outputPath", "output",
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.Text",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-outputFormat", "co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat"
				};
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(4,job.getOutput());
		
		FileSystem outputFS=getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Macon Kent,6269 Aenean St.,1-247-399-1051,08253");
		expectedOutput.add("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510");
		expectedOutput.add("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714");
		expectedOutput.add("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() + " and is it a dir? " + fileStat.isDirectory());
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
		assertEquals(4, count);
	} 
	
	@Test
	public void testMergeByValueWithSequenceFileAsTextInputFormat() throws Exception {
		HashMap<IntWritable, Text> inputData1 = new HashMap<IntWritable, Text>();
		inputData1.put(new IntWritable(1), new Text("Macon Kent,6269 Aenean St.,1-247-399-1051,08253"));
		inputData1.put(new IntWritable(2), new Text("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510"));
		inputData1.put(new IntWritable(3), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData1, "/input1", "testFile1.seq");
		
		HashMap<IntWritable, Text> inputData2 = new HashMap<IntWritable, Text>();
		inputData2.put(new IntWritable(1), new Text("Timon Leonard,716 Ac Ave,1-857-935-3882,62240"));
		inputData2.put(new IntWritable(2), new Text("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584"));
		inputData2.put(new IntWritable(4), new Text("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714"));
		createSequenceFileInHdfs(inputData2, "/input2", "testFile2.seq");

		String[] args = new String[] {
				"-newPath", "/input1",
				"-oldPath",	"/input2",
				"-mergeBy", "value", 
				"-outputPath", "output",
				"-inputFormat",	"org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat",
				"-inputKeyClassName", "org.apache.hadoop.io.Text",
				"-inputValueClassName", "org.apache.hadoop.io.Text",
				"-outputFormat", "co.nubetech.hiho.mapreduce.lib.output.NoKeyOnlyValueOutputFormat"
				};
		MergeJob job=runMergeJobs(args);
		assertEquals(3,job.getTotalRecordsNew());
		assertEquals(3,job.getTotalRecordsOld());
		assertEquals(0,job.getBadRecords());
		assertEquals(5,job.getOutput());
		
		FileSystem outputFS=getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "output");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("Macon Kent,6269 Aenean St.,1-247-399-1051,08253");
		expectedOutput.add("Dale Zamora,521-7792 Mauris Rd.,1-214-625-6970,90510");
		expectedOutput.add("Charles Wood,525-9709 In Rd.,1-370-528-4758,62714");
		expectedOutput.add("Timon Leonard,716 Ac Ave,1-857-935-3882,62240");
		expectedOutput.add("Macaulay Jackson,5435 Dui. Avenue,1-770-395-6446,31584");
		int count = 0;
		for (FileStatus fileStat: status) {
			logger.debug("File status is " + fileStat.getPath() + " and is it a dir? " + fileStat.isDirectory());
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
	
	
	public MergeJob runMergeJobs(String[] args) throws Exception{
		MergeJob job = new MergeJob();
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);
		return job;
	}
}