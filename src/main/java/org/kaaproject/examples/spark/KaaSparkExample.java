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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.kaaproject.kaa.examples.powerplant.PowerReport;
import org.kaaproject.kaa.examples.powerplant.PowerSample;
import org.kaaproject.kaa.server.common.log.shared.KaaFlumeEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class KaaSparkExample {
	private static final Logger LOG = LoggerFactory.getLogger(KaaSparkLauncher.class);
	// We will process data in batches every 10 seconds.
	private static final Duration BATCH_DURATION = new Duration(TimeUnit.SECONDS.toMillis(10L));

	// Data reader that decodes Flume events into user-defined data structures.
	private static KaaFlumeEventReader<PowerReport> reader = new KaaFlumeEventReader<PowerReport>(PowerReport.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		// Initializing Spark streaming context
		JavaStreamingContext ssc = new JavaStreamingContext(new JavaSparkContext(new SparkConf()), BATCH_DURATION);

		// Creating Flume stream to consume the data
		LOG.info("Binding flume stream to {}:{}", args[0], args[1]);
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(ssc, args[0], Integer.parseInt(args[1]));

		// Decode and map incoming events to <ZoneID, ZoneStats> pairs
		JavaPairDStream<Integer, ZoneStats> zoneVoltageDstream = flumeStream
		        .flatMapToPair(new PairFlatMapFunction<SparkFlumeEvent, Integer, ZoneStats>() {

			        @Override
			        public Iterable<Tuple2<Integer, ZoneStats>> call(SparkFlumeEvent sparkFlumeEvent) throws Exception {
				        List<Tuple2<Integer, ZoneStats>> results = new ArrayList<Tuple2<Integer, ZoneStats>>();

				        // Iterating through each event
				        for (PowerReport record : reader.decodeRecords(sparkFlumeEvent.event().getBody())) {
					        LOG.info("Parsed record: {}", record);
					        // Iterating through per panel samples
					        for (PowerSample sample : record.getSamples()) {
						        results.add(new Tuple2<Integer, ZoneStats>(sample.getZoneId(), new ZoneStats(1, sample.getPower())));
					        }
				        }

				        LOG.info("Event parsed.");
				        return results;
			        }

		        });

		// Apply simple reduce function to all <ZoneID, ZoneStats> pairs in
		// order to calculate average and total power produced in each zone.
		zoneVoltageDstream
		        .reduceByKey(new Function2<KaaSparkExample.ZoneStats, KaaSparkExample.ZoneStats, KaaSparkExample.ZoneStats>() {

			        // Simple reduce function that calculates total panel count
			        // and
			        // total power produced in scope of each zone.
			        @Override
			        public ZoneStats call(ZoneStats v1, ZoneStats v2) throws Exception {
				        return new ZoneStats(v1.panelCount + v2.panelCount, v1.powerSum + v2.powerSum);
			        }
			        // Map results to string for pretty output
		        }).transformToPair(new Function<JavaPairRDD<Integer, ZoneStats>, JavaPairRDD<Integer, ZoneStats>>() {

			        @Override
			        public JavaPairRDD<Integer, ZoneStats> call(JavaPairRDD<Integer, ZoneStats> v1) throws Exception {
				        return v1.sortByKey();
			        }
		        }).map(new Function<Tuple2<Integer, ZoneStats>, String>() {

			        @Override
			        public String call(Tuple2<Integer, ZoneStats> tuple) throws Exception {
				        StringBuilder sb = new StringBuilder();
				        sb.append("Zone ").append(tuple._1()).append(": ");
				        sb.append("Total power ").append(tuple._2().getTotalPower()).append(" collected from ")
				                .append(tuple._2().getPanelCount()).append(" panels. ");
				        sb.append("Average power produced by each panel is ").append(tuple._2().getAvgPower());
				        return sb.toString();
			        }
		        }).print();

		// Start streaming application
		ssc.start();
		// Block until terminated
		ssc.awaitTermination();
	}

	// Simple class that is used in calculation and implements Serializable
	// interface.
	private static final class ZoneStats implements Serializable {
		private static final long serialVersionUID = 1L;

		private final int panelCount;
		private final double powerSum;

		public ZoneStats(int panelCount, double voltageSum) {
			super();
			this.panelCount = panelCount;
			this.powerSum = voltageSum;
		}

		public int getPanelCount() {
			return panelCount;
		}

		public String getTotalPower() {
			return String.format("%.2f kW", powerSum);
		}

		public String getAvgPower() {
			return String.format("%.2f kW", powerSum / panelCount);
		}
	}
}
