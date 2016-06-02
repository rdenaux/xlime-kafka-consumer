package eu.xlime.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
 
/**
 * Provides a way to launch (and request shutdown) of {@link KafkaStreamConsumer}s 
 * for Kafka topics using multithreading.
 *  
 * @author rdenaux
 */
public class KafkaStreamConsumerLauncher {
	private static final Logger log = LoggerFactory.getLogger(KafkaStreamConsumerLauncher.class);
	
	private final ConsumerConnector consumer;
	
    private ExecutorService executor;
    private final Map<String, List<Future<?>>> launchedTopicStreamConsumers;
    private final Properties cfgProps;
    private Map<String, Integer> topicCountMap;
    private Map<String, List<KafkaStream<byte[], byte[]>>> topicsToStreams;
    
    public KafkaStreamConsumerLauncher(Properties aCfgProperties) {
    	cfgProps = aCfgProperties;
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(cfgProps));
        topicCountMap = calcTopicCountMap();
        launchedTopicStreamConsumers = new HashMap<String, List<Future<?>>>();
    }
 
    private Map<String, Integer> calcTopicCountMap() {
        List<String> topics = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPICS.getList(cfgProps);
        log.info("Readying to launch consumers for topics " + topics);
        Map<String, Integer> result = new HashMap<String, Integer>();
        for (String topic: topics) {
        	int threads = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_THREADS.getIntValue(cfgProps, topic);
        	result.put(topic, threads);
        }
		return ImmutableMap.copyOf(result);
	}

    /**
     * Checks whether the {@link #launchStreamConsumers()} are still running. If not, takes action, based on 
     * configuration options (i.e. restart or not).
     */
	public void monitorConsumers() {
		log.info(String.format("%s consumers running out of %s", countRunningConsumers(), totalThreads()));
		if (executor.isShutdown()) return;
    	for (String topic: launchedTopicStreamConsumers.keySet()) {
    		String action = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_ON_STOP.getValue(cfgProps, topic);
    		if (!"restart".equalsIgnoreCase(action)) continue;
    		// restart if stopped
    		List<Future<?>> newFutures = new ArrayList<Future<?>>();
    		List<Future<?>> futures = launchedTopicStreamConsumers.get(topic);
    		boolean replace = false;
    		int threadNum = 0;
    		for (Future<?> f: futures) {
    			if (f.isDone()) {
    				log.info(String.format("Trying to restat stream consumer for %s_%s", topic, threadNum));
    				Optional<Future> optNewF = tryRestartStreamConsumer(topic,
							threadNum);
    				newFutures.add(optNewF.or(f)); 
    				replace |= optNewF.isPresent();
    			} else newFutures.add(f);
    			threadNum++;
    		}
    		if (replace)
    			launchedTopicStreamConsumers.put(topic, newFutures);
    	}
	}

	private Optional<Future> tryRestartStreamConsumer(String topic,
			int threadNum) {
		try {
			KafkaStream stream = topicsToStreams.get(topic).get(threadNum);
			Future<?> newF = executor.submit(new KafkaStreamConsumer(cfgProps, stream, threadNum, topic));
			return Optional.<Future>fromNullable(newF);
		} catch (Exception e) {
			log.error(String.format("Failed to restart consumer for %s_%s", topic, threadNum), e);
			return Optional.absent();
		}
	}
    

    public int countRunningConsumers() {
    	int count = 0;
    	for (String topic: launchedTopicStreamConsumers.keySet()) {
    		List<Future<?>> futures = launchedTopicStreamConsumers.get(topic);
    		int thread = 0;
    		for (Future<?> f: futures) {
    			if (f.isDone()) {
    				log.debug(String.format("StreamConsumer %s_%s has finished.", topic, thread));
    			} else count++;
    			thread++;
    		}
    	}
    	return count;
    }

    /**
     * Only when all topic consumers have finished and are configured to exit on stop.
     * @return
     */
	public boolean canExit() {
		int runningConsumers = countRunningConsumers();
		log.info(String.format("%s consumers running out of %s", runningConsumers, totalThreads()));
		if (runningConsumers > 0) return false;
		Set<String> actions = new HashSet<String>();
    	for (String topic: launchedTopicStreamConsumers.keySet()) {
    		actions.add(ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_ON_STOP.getValue(cfgProps, topic).trim().toLowerCase());
    	}
    	boolean result = actions.equals(ImmutableSet.of("exit"));
    	log.info("Actions on stop for topics: " + actions);
    	return result;
	}
 
    
    
	/**
     * Initiates shutdown of the threads created by previous {@link #launchStreamConsumers(String, int)} 
     * invokations as well as shutting the {@link ConsumerConnector}. 
     */
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
    	if (executor != null) {
    		executor.shutdown();
            try {
				executor.awaitTermination(20, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.error("Error waiting for executor", e);
			}
    	}
		log.info("Shutdown Kafka consumer connector");
    }
 
    /**
     * Launches <code>aNumThreads</code> {@link KafkaStreamConsumer}s for a (kafka) topic. 
     * These consumers 
     *   
     * @param aTopic a kafka topic name
     * @param aNumThreads the number of stream consumers to launch (each one is launched in its own {@link Thread}.
     */
    public void launchStreamConsumers() {
    	if (executor != null) {
    		log.warn("Streams already launched");
    		return;
    	}
        topicsToStreams = consumer.createMessageStreams(topicCountMap);
    	
        int aNumThreads = totalThreads();
        log.info(String.format("Creating thread pool with %s threads to read topics %s.", aNumThreads, topicCountMap));
        executor = Executors.newFixedThreadPool(aNumThreads, newThreadFactory("topic"));  // create thread executor
        
         // launch stream consumer threads
        for (String topic: topicsToStreams.keySet()) {
        	int threadNumber = 0;
        	List<Future<?>> launchedFutures = new ArrayList<Future<?>>();
        	for (final KafkaStream stream : topicsToStreams.get(topic)) {
        		Future<?> f = executor.submit(new KafkaStreamConsumer(cfgProps, stream, threadNumber, topic));
        		launchedFutures.add(f);
        		threadNumber++;
        	}
    		launchedTopicStreamConsumers.put(topic, launchedFutures);
        }
    }

	private int totalThreads() {
		int result = 0;
		for (int topicThreadCount: topicCountMap.values()) {
			result += topicThreadCount;
		}
		return result;
	}

	private ThreadFactory newThreadFactory(final String aTopic) {
		return new ThreadFactory() {
			int threadCount = 0;
			public Thread newThread(Runnable r) {
				Thread result = Executors.defaultThreadFactory().newThread(r);
				result.setName(String.format("kafka-stream-%s-consumer-%s", aTopic, threadCount++));
				return result;
			}
		};
	}
 
	private static ConsumerConfig createConsumerConfig(Properties cfgProps) {
		return new ConsumerConfigBuilder(cfgProps)
			.put("zookeeper.connect", ConfigOptions.XLIME_KAFKA_CONSUMER_ZOOKEEPER_CONNECT)
			.optPut("group.id", ConfigOptions.XLIME_KAFKA_CONSUMER_GROUP_ID)
			.optPut("zookeeper.session.timeout.ms", ConfigOptions.XLIME_KAFKA_CONSUMER_ZOOKEEPER_SESSION_TIMEOUT_MS)
			.optPut("zookeeper.sync.time.ms", ConfigOptions.XLIME_KAFKA_CONSUMER_ZOOKEEPER_SYNC_TIME_MS)
			.optPut("auto.commit.interval.ms", ConfigOptions.XLIME_KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS)
			.optPut("auto.offset.reset", ConfigOptions.XLIME_KAFKA_CONSUMER_AUTO_OFFSET_RESET)
			.build();
	}
	
	/**
	 * Used to create a Kafka {@link ConsumerConfig} instance from a {@link ConfigOptions}-based {@link Properties}
	 * instance.
	 * @author rdenaux
	 *
	 */
	static class ConsumerConfigBuilder {
		private final Properties props = new Properties();
		
		private final Properties inProps;
		
		public ConsumerConfigBuilder(Properties inProps) {
			super();
			this.inProps = inProps;
		}

		public ConsumerConfig build() {
			return new ConsumerConfig(props);
		}
		
		public ConsumerConfigBuilder optPut(String key, ConfigOptions cfgOpt) {
	        Optional<String> optVal = cfgOpt.getOptVal(inProps);
	        if (optVal.isPresent())
	        	props.put(key, optVal.get());
	        return this;
		}
		
		public ConsumerConfigBuilder put(String key, ConfigOptions cfgOpt) {
			props.put(key, cfgOpt.getValue(inProps));
			return this;
		}
	}
	
	@Deprecated //use version using Properties and ConfigOptions input
	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
//        props.put("auto.offset.reset", "largest"); 
        
        return new ConsumerConfig(props);
    }
    

}
