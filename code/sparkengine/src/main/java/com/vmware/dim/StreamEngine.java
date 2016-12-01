package com.vmware.dim;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import kafka.serializer.StringDecoder;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.vmware.common.dim.DataTransformation;
import com.vmware.common.dim.config.AppConfig;

/**
 * read from a folder. Keep adding new file with data into the folder specified.
 */
public final class StreamEngine {
	private static final Logger logger = LogManager.getLogger(StreamEngine.class);

	public static void main(String[] args) throws Exception {
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
		org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);

		Gson gson = new GsonBuilder().create();
		String configurationJson = "";
		logger.debug("Reading configuration file");
		if (args[0].trim().startsWith("{")) {
			configurationJson = args[0];
		} else {
			configurationJson = FileUtils.readFileToString(new File(args[0]));
		}

		logger.info(configurationJson);
		AppConfig ac = gson.fromJson(configurationJson, AppConfig.class);
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(ac.checkpoint,
				new Function0<JavaStreamingContext>() {

					private static final long serialVersionUID = 1L;

					@Override
					public JavaStreamingContext call() throws Exception {
						return createContext(ac);
					}
				});

		ssc.start();
		ssc.awaitTermination();
	}

	public static JavaStreamingContext createContext(AppConfig ac) {
		
		SparkConf sparkConf = new SparkConf().setAppName(ac.AppName);
//		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(ac.AppName);
		
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(ac.batchSize));
		ssc.checkpoint(ac.checkpoint);

		HashSet<String> topicsSet = new HashSet<String>();
		topicsSet.add(ac.kafka.streamSourceTopics);

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", ac.kafka.streamSourceBrokers);

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(ssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		JavaDStream<Map<String, String>> lines = messages
				.map(new Function<Tuple2<String, String>, Map<String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, String> call(Tuple2<String, String> tuple2) {
						String datapayload = new String(tuple2._2());
						logger.info("## Data Payload" + datapayload);

						try {
							// Data Processing Logic
							DataTransformation.processStreamPacket(datapayload);
							logger.debug("Completed Packet");
						} catch (Exception e) {
							logger.error(
									"Stream Processing encountered fatal error. Discarding current Stream and moving on.",
									e);
						}

						Map<String, String> ret = new HashMap<String, String>();
						ret.put(UUID.randomUUID().toString(), tuple2._2());
						return ret;
					}
				});

		lines.foreachRDD(new VoidFunction<JavaRDD<Map<String, String>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Map<String, String>> arg0) throws Exception {
				System.out.println(arg0.count());

			}
		});

		return ssc;
	}
}