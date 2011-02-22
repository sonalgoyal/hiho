/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.nubetech.hiho.common;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;


import org.apache.hadoop.fs.Path;
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

	
	
	// pass through to the junit 3 calls, which are not annotated
    @Before 
    public void setUp() throws Exception {
    	setupDefaultSystemPropertiesIf();
    	System.out.println("Inside setup");
        super.setUp();
        
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
