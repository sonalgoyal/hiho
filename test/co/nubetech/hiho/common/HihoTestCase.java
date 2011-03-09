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
package co.nubetech.hiho.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

/**
 * Bridge between Junit 3 and 4. Bridge between
 * 
 * @author sgoyal
 * 
 */

@RunWith(JUnit4ClassRunner.class)
public abstract class HihoTestCase extends ClusterMapReduceTestCase {
	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.common.HihoTestCase.class);

	private OutputPathFilter outputPathFilter;

	public OutputPathFilter getOutputPathFilter() {
		return outputPathFilter;
	}

	public void setOutputPathFilter(OutputPathFilter outputPathFilter) {
		this.outputPathFilter = outputPathFilter;
	}

	public static final class OutputPathFilter implements PathFilter {

		public boolean accept(Path path) {
			return (path.getName().startsWith("part-"));
		}
	}

	// pass through to the junit 3 calls, which are not annotated
	@Before
	public void setUp() throws Exception {
		setupDefaultSystemPropertiesIf();
		System.out.println("Inside setup");
		super.setUp();
		outputPathFilter = new OutputPathFilter();

		System.out.println("System.getProperty(hadoop.log.dir) = "
				+ System.getProperty("hadoop.log.dir"));
		System.out.println("System.getProperty(hadoop.log.file) = "
				+ System.getProperty("hadoop.log.file"));
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	public static void setupDefaultSystemPropertiesIf() {
		if (System.getProperty("hadoop.log.dir") == null) {
			System.setProperty("hadoop.log.dir",
					System.getProperty("java.io.tmpdir", "."));
		}
		if (System.getProperty("hadoop.log.file") == null) {
			System.setProperty("hadoop.log.file", "hadoop.log");
		}
		if (System.getProperty("hadoop.root.logger") == null) {
			System.setProperty("hadoop.root.logger", "DEBUG,console");
		}
		LogManager.getRootLogger().setLevel(Level.toLevel("DEBUG"));
		LogManager.getLogger("org.mortbay").setLevel(Level.toLevel("WARN"));
		LogManager.getLogger("co.nubetech").setLevel(Level.toLevel("DEBUG"));
		// LOG.setLevel(Level.toLevel(testLogLevel));

	}

	public void createTextFileInHDFS(String inputData, String filePath,
			String nameOfFile) throws IOException {
		FileSystem fs = getFileSystem();
		FSDataOutputStream out = null;
		Path inputFile = new Path(filePath + "/" + nameOfFile);
		try {
			out = fs.create(inputFile, false);
			out.write(inputData.getBytes(), 0, inputData.getBytes().length);
			out.close();
			out = null;
			// Cheking input file exists or not.
			Path inputPath = new Path(fs.getHomeDirectory(), filePath + "/"
					+ nameOfFile);
			assertTrue(fs.exists(inputPath));
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	public void createSequenceFileInHdfs(HashMap inputData, String filePath,
			String nameOfFile) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = getFileSystem();
		Path inputFile = new Path(filePath + "/" + nameOfFile);
		SequenceFile.Writer writer = null;
		SequenceFile.Reader reader = null;
		try {
			Set key = inputData.keySet();
			Object keyValue = key.iterator().next();
			writer = SequenceFile.createWriter(fs, conf, inputFile,
					keyValue.getClass(), inputData.get(keyValue).getClass());
			logger.debug("key class is: " + keyValue.getClass());
			logger.debug("val class is: " + inputData.get(keyValue).getClass());
			Iterator valIterator = inputData.values().iterator();
			Iterator keyIterator = inputData.keySet().iterator();
			while (keyIterator.hasNext()) {
				writer.append(keyIterator.next(), valIterator.next());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * @Test
	 * 
	 *       public void test() { System.out.println(
	 *       "A cute little test to ensure thigns are ok with the cluster"); }
	 */

}
