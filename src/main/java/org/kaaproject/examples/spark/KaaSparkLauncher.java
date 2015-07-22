/*
 * Copyright 2014-2015 CyberVision, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kaaproject.examples.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaaSparkLauncher {
	
	private static final Logger LOG = LoggerFactory.getLogger(KaaSparkLauncher.class);
	
	
	/*
	 * Environment specific constants.
	 * TODO: replace YOUR_HOST, YOUR_SPARK_HOME and YOUR_WORK_DIR
	 */
	private static final String FLUME_BIND_HOST = "$YOUR_HOST$";
	private static final String FLUME_BIND_PORT = "7070";	
	private static final String SPARK_MASTER_URL = "spark://$YOUR_HOST$:7077";
	private static final String SPARK_HOME = "$YOUR_SPARK_HOME$";
	private static final String SPARK_APP_JAR = "$YOUR_WORK_DIR$/spark/target/job.jar";
	
	private static final String KAA_SPARK_EXAMPLE_JOB_NAME = "KaaSparkExample";

	public static void main(String[] args) throws Exception {
		SparkLauncher launcher = new SparkLauncher()
				.setMaster(SPARK_MASTER_URL).setSparkHome(SPARK_HOME).setAppResource(SPARK_APP_JAR)
		        .setMainClass(KaaSparkExample.class.getName()).setAppName(KAA_SPARK_EXAMPLE_JOB_NAME)
		        .addAppArgs(FLUME_BIND_HOST, FLUME_BIND_PORT);

		final Process spark = launcher.launch();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOG.warn("Spark job interrupted!");
				spark.destroy();
			}
		});

		Thread isReader = startReader(spark.getInputStream());
		Thread esReader = startReader(spark.getErrorStream());

		int resultCode = spark.waitFor();

		isReader.join();
		esReader.join();

		if (resultCode != 0) {
			LOG.warn("Spark job result code: {}", resultCode);
		}
	}

	private static Thread startReader(final InputStream is) {
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is));
				String line;
				try {
					while ((line = reader.readLine()) != null) {
						LOG.debug(line);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		});

		t.start();
		return t;
	}

}
