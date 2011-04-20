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
package co.nubetech.hiho.similarity.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.nubetech.hiho.common.HIHOException;

public class SimilarityJob {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.similarity.ngram.NGramJob.class);

	private String inputPath = null;

	public void populateConfiguration(String[] args) {
		for (int i = 0; i < args.length - 1; i++) {
			if ("-inputPath".equals(args[i])) {
				inputPath = args[++i];
			}
		}
	}

	public void checkMandatoryConfs() throws HIHOException {
		if (inputPath == null) {
			throw new HIHOException(
					"The provided input path is empty, please specify inputPath");
		}
	}

	public void run(String[] args) throws Exception {
		
		populateConfiguration(args);
		try {
			checkMandatoryConfs();
		} catch (HIHOException e1) {
			e1.printStackTrace();
			throw new Exception(e1);
		}		
		
		String[] nGramJobArgs =  new String[] {"-inputPath", inputPath};		
		ToolRunner.run(new Configuration(), new NGramJob(), nGramJobArgs);
		
		String[] scoreJobArgs =  new String[] {};
		ToolRunner.run(new Configuration(), new ScoreJob(), scoreJobArgs);
	}

	public static void main(String[] args) throws Exception {
		SimilarityJob job = new SimilarityJob();
		job.run(args);
	}
}
