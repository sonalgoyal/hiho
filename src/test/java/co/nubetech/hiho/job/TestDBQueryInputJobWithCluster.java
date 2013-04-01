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
package co.nubetech.hiho.job;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.hsqldb.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import co.nubetech.hiho.common.HIHOException;
import co.nubetech.hiho.common.HihoTestCase;

public class TestDBQueryInputJobWithCluster extends HihoTestCase {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.job.TestDBQueryInputJobWithCluster.class);

	private static final String DB_URL = "jdbc:hsqldb:hsql://localhost/URLAccess";
	private static final String DRIVER_CLASS = "org.hsqldb.jdbcDriver";
	private static Server server;
	private static Connection connection;	
	
	
	@Test
	public void testBasicTableImport() throws Exception{
		DBQueryInputJob job = new DBQueryInputJob();
		
		String[] args = new String[] {"-jdbcDriver", "org.hsqldb.jdbcDriver", 
				"-jdbcUrl", "jdbc:hsqldb:hsql://localhost/URLAccess",
			//	"-jdbcUsername", "",
			//	"-jdbcPassword", "",
				"-outputPath", "testBasicTableImport",
				"-outputStrategy", "delimited",
				"-delimiter", "DELIM",
				"-numberOfMappers", "2",
				"-inputTableName", "Pageview",
				"-inputOrderBy", "pageview"
		};
		int res = ToolRunner.run(createJobConf(), job, args); 
		assertEquals(0, res);
		//lets verify the result now
		FileSystem outputFS = getFileSystem();
		//Path outputPath = getOutputDir();
		
		Path outputPath = new Path(outputFS.getHomeDirectory(), "testBasicTableImport");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("/aDELIM1000");
		expectedOutput.add("/bDELIM2000");
		expectedOutput.add("/cDELIM3000");
		expectedOutput.add("/dDELIM4000");
		expectedOutput.add("/eDELIM5000");
		expectedOutput.add("/fDELIM6000");
		expectedOutput.add("/gDELIM7000");
		expectedOutput.add("/hDELIM8000");
		expectedOutput.add("/iDELIM9000");
		expectedOutput.add("/jDELIM10000");
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
		assertEquals(10, count);			
	}
	
	//Still to complete
	@Test
	public void testBasicAvroTableImport() throws Exception{
		DBQueryInputJob job = new DBQueryInputJob();
		
		String[] args = new String[] {"-jdbcDriver", "org.hsqldb.jdbcDriver", 
				"-jdbcUrl", "jdbc:hsqldb:hsql://localhost/URLAccess",
				"-outputPath", "testQueryBasedImport",
				"-inputQuery", "select url,pageview,commentCount from Pageview, PageComment where Pageview.url = PageComment.url",
				"-inputBoundingQuery", "select min(commentCount), max(commentCount) from PageComment",
				"-outputStrategy", "AVRO",
				"-delimiter", "DELIM",
				"-numberOfMappers", "2",
				"-inputOrderBy", "Pageview.pageview"
		};
		int res = ToolRunner.run(createJobConf(), job, args); 
		assertEquals(0, res);
		//lets verify the result now
		FileSystem outputFS = getFileSystem();
		//Path outputPath = getOutputDir();
		
		Path outputPath = new Path(outputFS.getHomeDirectory(), "testBasicTableImport");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
	/*	List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("/aDELIM1000");
		expectedOutput.add("/bDELIM2000");
		expectedOutput.add("/cDELIM3000");
		expectedOutput.add("/dDELIM4000");
		expectedOutput.add("/eDELIM5000");
		expectedOutput.add("/fDELIM6000");
		expectedOutput.add("/gDELIM7000");
		expectedOutput.add("/hDELIM8000");
		expectedOutput.add("/iDELIM9000");
		expectedOutput.add("/jDELIM10000");
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
		assertEquals(10, count);	*/		
	}
	
	@Test
	public void testQueryBasedImport() throws Exception{
		DBQueryInputJob job = new DBQueryInputJob();
		
		String[] args = new String[] {"-jdbcDriver", "org.hsqldb.jdbcDriver", 
				"-jdbcUrl", "jdbc:hsqldb:hsql://localhost/URLAccess",
				"-outputPath", "testQueryBasedImport",
				"-inputQuery", "select url,pageview,commentCount from Pageview, PageComment where Pageview.url = PageComment.url",
				"-inputBoundingQuery", "select min(commentCount), max(commentCount) from PageComment",
				"-outputStrategy", "delimited",
				"-delimiter", "DELIM",
				"-numberOfMappers", "2",
				"-inputOrderBy", "Pageview.pageview"
		};
		int res = ToolRunner.run(createJobConf(), job, args); 
		assertEquals(0, res);
		//lets verify the result now
		FileSystem outputFS = getFileSystem();
		Path outputPath = new Path(outputFS.getHomeDirectory(), "testQueryBasedImport");
		FileStatus[] status = outputFS.listStatus(outputPath, getOutputPathFilter()); 
		assertTrue(outputFS.exists(outputPath));
		List<String> expectedOutput = new ArrayList<String>();
		expectedOutput.add("/aDELIM1000DELIM10");
		expectedOutput.add("/bDELIM2000DELIM10");
		expectedOutput.add("/cDELIM3000DELIM10");
		expectedOutput.add("/dDELIM4000DELIM10");
		expectedOutput.add("/eDELIM5000DELIM10");
		expectedOutput.add("/fDELIM6000DELIM10");
		expectedOutput.add("/gDELIM7000DELIM10");
		expectedOutput.add("/hDELIM8000DELIM10");
		expectedOutput.add("/iDELIM9000DELIM10");
		expectedOutput.add("/jDELIM10000DELIM10");
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
		assertEquals(10, count);			
	}


	@Test
	public void testGeneratePigScript() throws Exception, HIHOException {
		DBQueryInputJob job = new DBQueryInputJob();
		String[] args = new String[] {
				"-jdbcDriver",	"org.hsqldb.jdbcDriver",
				"-jdbcUrl",	"jdbc:hsqldb:hsql://localhost/URLAccess",
				"-inputLoadTo", "pig",
				"-inputLoadToPath", "/tmp",
				"-outputPath", "output",
				"-outputStrategy", "delimited",
				"-delimiter", ",",
				"-numberOfMappers", "2",
				"-inputTableName", "Pageview" , 
				"-inputOrderBy", "pageview" };
		int res = ToolRunner.run(createJobConf(), job, args);
		assertEquals(0, res);
		// lets verify the result now
		File pigScript = new File("/tmp/pigScript.txt");			
		if(!(pigScript.exists())){
			throw new HIHOException("Unable to generate Pig script");
		}
		logger.debug("Pig script output is  " + pigScript.exists());		
	}
	
	
	//still to complete.
	@Test
	public void testContentOfPigScript() throws ExecException, IOException {
		PigServer pigServer = new PigServer("LOCAL");
		pigServer.registerScript("/tmp/pigScript.txt");
		//pigServer.registerQuery("A = LOAD 'mapreduce.jdbc.hiho.input.outputPath' USING PigStorage(',') AS (URL:chararray,PAGEVIEW:long);");
		/*pigServer.dumpSchema("A") ;
		String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(new File("stdout.redirected"))));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            assertTrue(s.equals("a: {field1: int,field2: float,field3: chararray}") == true);
        }
        reader.close();*/
	}


	private static void startHsqldbServer() {
		server = new Server();
		server.setDatabasePath(0, System.getProperty("test.build.data", "/tmp")
				+ "/URLAccess");
		server.setDatabaseName(0, "URLAccess");
		server.start();
	}

	private static void createConnection(String driverClassName, String url)
			throws Exception {

		Class.forName(driverClassName);
		connection = DriverManager.getConnection(url);
		connection.setAutoCommit(false);
	}

	@AfterClass
	public static void shutdown() {
		try {
			connection.commit();
			connection.close();
		} catch (Throwable ex) {
			logger.warn("Exception occurred while closing connection :"
					+ ex);
		} finally {
			try {
				if (server != null) {
					server.shutdown();
				}
			} catch (Throwable ex) {
				logger.warn("Exception occurred while shutting down HSQLDB :"
						+ ex);
			}
		}
	}

	@BeforeClass
	public static void initialize()
			throws Exception {
			startHsqldbServer();
			createConnection(DRIVER_CLASS, DB_URL);
			dropTables();
			createTables();
			populateAccess();
			verify();
	}

	private static void dropTables() {
		String dropAccess = "DROP TABLE Access";
		String dropPageview = "DROP TABLE Pageview";
		String dropPageComment="DROP Table PageComment";
		Statement st = null;
		try {
			st = connection.createStatement();
			st.executeUpdate(dropAccess);
			st.executeUpdate(dropPageview);
			st.executeUpdate(dropPageComment);
			connection.commit();
			st.close();
		} catch (SQLException ex) {
			try {
				if (st != null) {
					st.close();
				}
			} catch (Exception e) {
			}
		}
	}

	private static void createTables() throws SQLException {

		String createAccess = "CREATE TABLE "
				+ "Access(url      VARCHAR(100) NOT NULL,"
				+ " referrer VARCHAR(100)," + " time     BIGINT NOT NULL, "
				+ " PRIMARY KEY (url, time))";

		String createPageview = "CREATE TABLE "
				+ "Pageview(url      VARCHAR(100) NOT NULL,"
				+ " pageview     BIGINT NOT NULL, " + " PRIMARY KEY (url))";
		
		String createPageComment = "CREATE TABLE "
			+ "PageComment(url      VARCHAR(100) NOT NULL,"
			+ " commentCount     BIGINT NOT NULL, " + " PRIMARY KEY (url))";

		Statement st = connection.createStatement();
		try {
			st.executeUpdate(createAccess);
			st.executeUpdate(createPageview);
			st.executeUpdate(createPageComment);
			connection.commit();
		} finally {
			st.close();
		}
	}

	/**
	 * Populates the Access table with generated records.
	 */
	private static void populateAccess() throws SQLException {

		PreparedStatement statement = null;
		try {
			statement = connection
					.prepareStatement("INSERT INTO Access(url, referrer, time)"
							+ " VALUES (?, ?, ?)");

			Random random = new Random();

			int time = random.nextInt(50) + 50;

			final int PROBABILITY_PRECISION = 100; // 1 / 100
			final int NEW_PAGE_PROBABILITY = 15; // 15 / 100

			// Pages in the site :
			String[] pages = { "/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h",
					"/i", "/j" };
			// linkMatrix[i] is the array of pages(indexes) that page_i links
			// to.
			int[][] linkMatrix = { { 1, 5, 7 }, { 0, 7, 4, 6, },
					{ 0, 1, 7, 8 }, { 0, 2, 4, 6, 7, 9 }, { 0, 1 },
					{ 0, 3, 5, 9 }, { 0 }, { 0, 1, 3 }, { 0, 2, 6 },
					{ 0, 2, 6 } };

			// a mini model of user browsing a la pagerank
			int currentPage = random.nextInt(pages.length);
			String referrer = null;

			for (int i = 0; i < time; i++) {

				statement.setString(1, pages[currentPage]);
				statement.setString(2, referrer);
				statement.setLong(3, i);
				statement.execute();

				int action = random.nextInt(PROBABILITY_PRECISION);

				// go to a new page with probability
				// NEW_PAGE_PROBABILITY / PROBABILITY_PRECISION
				if (action < NEW_PAGE_PROBABILITY) {
					currentPage = random.nextInt(pages.length); // a random page
					referrer = null;
				} else {
					referrer = pages[currentPage];
					action = random.nextInt(linkMatrix[currentPage].length);
					currentPage = linkMatrix[currentPage][action];
				}
			}
			
			PreparedStatement statement1 = connection
				.prepareStatement("INSERT INTO Pageview(url, pageview)"
					+ " VALUES (?, ?)");
			
			int i = 1;
			for (String page: pages) {
				statement1.setString(1, page);
				statement1.setInt(2, 1000*i);
				i++;
				statement1.execute();
			}
			
			PreparedStatement statement2 = connection
			.prepareStatement("INSERT INTO PageComment(url, commentCount)"
				+ " VALUES (?, ?)");
		
		int j = 1;
		for (String page: pages) {
			statement2.setString(1, page);
			statement2.setInt(2, 10*j);
			i++;
			statement2.execute();
		}

			connection.commit();

		} catch (SQLException ex) {
			connection.rollback();
			throw ex;
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
	}

	/** Verifies the results are correct */
	private static boolean verify() throws SQLException {
		// check total num pageview
		String countAccessQuery = "SELECT COUNT(*) FROM Access";
		String sumPageviewQuery = "SELECT SUM(pageview) FROM Pageview";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = connection.createStatement();
			rs = st.executeQuery(countAccessQuery);
			rs.next();
			long totalPageview = rs.getLong(1);

			rs = st.executeQuery(sumPageviewQuery);
			rs.next();
			long sumPageview = rs.getLong(1);

			logger.info("totalPageview=" + totalPageview);
			logger.info("sumPageview=" + sumPageview);

			return totalPageview == sumPageview && totalPageview != 0;
		} finally {
			if (st != null)
				st.close();
			if (rs != null)
				rs.close();
		}
	}
	
	
}
