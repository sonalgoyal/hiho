package co.nubetech.hiho.common;

import java.io.FileFilter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.runner.RunWith;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;


/**
 * Bridge between Junit 3 and 4. Bridge between  
 * @author sgoyal
 *
 */

@RunWith(JUnit4ClassRunner.class) 
public abstract class HihoTestCase extends ClusterMapReduceTestCase{
	
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
        
        System.out.println("System.getProperty(hadoop.log.dir) = " + System.getProperty("hadoop.log.dir"));
        System.out.println("System.getProperty(hadoop.log.file) = " + System.getProperty("hadoop.log.file"));
    }

    @After 
    public void tearDown() throws Exception {
        super.tearDown();
    }
    
    public static void setupDefaultSystemPropertiesIf() {
		if (System.getProperty("hadoop.log.dir")==null) {
			System.setProperty("hadoop.log.dir", System.getProperty("java.io.tmpdir","."));
		}
		if (System.getProperty("hadoop.log.file")==null) {
			System.setProperty("hadoop.log.file", "hadoop.log");
		}
		if  (System.getProperty("hadoop.root.logger")==null) {
			System.setProperty("hadoop.root.logger", "DEBUG,console");
		}
		LogManager.getRootLogger().setLevel(Level.toLevel("DEBUG"));
		LogManager.getLogger("org.mortbay").setLevel(Level.toLevel("WARN"));
		LogManager.getLogger("co.nubetech").setLevel(Level.toLevel("DEBUG"));
//		LOG.setLevel(Level.toLevel(testLogLevel));
		
	}   
    
    /**
     * @Test
     
    public void test() {
    	System.out.println("A cute little test to ensure thigns are ok with the cluster");
    }
    */
   

}
