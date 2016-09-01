package eu.xlime.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public enum ConfigOptions {

	XLIME_KAFKA_CONSUMER_ZOOKEEPER_CONNECT(//
			"xlime.kafka.consumer.zookeeper.connect",
			"The 'host:port' string to connect to Kafka's zookeeper. No default value.",
			"", String.class),//
	XLIME_KAFKA_CONSUMER_GROUP_ID(//
			"xlime.kafka.consumer.group.id",
			"The consumer group id to use when connecting to Kafka. See Kafka's documentation. No default value.",
			null, String.class), // 
	XLIME_KAFKA_CONSUMER_ZOOKEEPER_SESSION_TIMEOUT_MS(//
			"xlime.kafka.consumer.zookeeper.session.timeout.ms",
			"Zookeeper session timeout in milliseconds. If the consumer fails to heartbeat to zookeeper for this period of time it is considered dead and a rebalance will occur. Default is 6000",
			"6000", Long.class), //
	XLIME_KAFKA_CONSUMER_ZOOKEEPER_SYNC_TIME_MS(//
			"xlime.kafka.consumer.zookeeper.sync.time.ms",
			"How far a ZK follower can be behind a ZK leader in milliseconds. Default is 2000",
			"2000", Long.class), //
	XLIME_KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS(//
			"xlime.kafka.consumer.auto.commit.interval.ms",
			"The frequency in ms that the consumer offsets are committed to zookeeper. Default is 60000",
			"60000", Long.class),// 
	XLIME_KAFKA_CONSUMER_AUTO_OFFSET_RESET(//
			"xlime.kafka.consumer.auto.offset.reset",
			"Strategy to use when resetting offset when connecting. Possible values 'smallest', 'largest'. Default 'smallest'",
			"smallest", String.class),
	XLIME_KAFKA_CONSUMER_TIMED_RUN_S(//
			"xlime.kafka.consumer.timed.run.s",
			"Only execute the consumer(s) for a maximum of n seconds. No default value (i.e. consume messages indefinetely).",
			null, Integer.class	),// 
	XLIME_KAFKA_CONSUMER_TOPICS(//
			"xlime.kafka.consumer.topics",
			"Comma separated list of topics to consume",
			"", List.class), 
	XLIME_KAFKA_CONSUMER_TOPIC_THREADS(//
			"xlime.kafka.consumer.topic.%s.threads",
			"Number of threads to use to consume the specified thread",
			"1", Integer.class), 
	XLIME_KAFKA_CONSUMER_TOPIC_MAX_COUNT_CONSUMED(//
			"xlime.kafka.consumer.topic.%s.max-count-consumed",
			"Maximum number of messges to consume (per thread!). If not specified, keep consuming messages.",
			null, Long.class),// 
	XLIME_KAFKA_CONSUMER_TOPIC_ON_STOP(//
			"xlime.kafka.consumer.topic.%s.on-stop",
			"What to do when a stream consumer thread finishes, options are 'restart' (launch a new consumer) 'exit' (do not restart, when all consumers are finished, the application exits). Default is 'restart'.",
			"restart", String.class),
	XLIME_KAFKA_CONSUMER_TOPIC_RDF_DATASET_PROCESSOR(//
			"xlime.kafka.consumer.topic.%s.rdf.dataset-processor.fqn",
			"Fully qualified name of the class implementing the DatasetProcessor interface that will process the RDF message.",
			null, String.class),
	XLIME_KAFKA_CONSUMER_TOPIC_RDF_DATASET_PROCESSOR_SUMMARISE_EVERY(
			"xlime.kafka.consumer.topic.rdf.dataset-processor.summarise-every",
			"Indicate that DataProcessors should log a summary of their processing every n messages. Default is 500. A negative number indicates that no summary should be logged.",
			"500", Long.class)
	;
	
	final String key;

	final String description;

	final String defaultValue;

	final Class<?> type;

	private ConfigOptions(String aKey, String aDesc, String aDefaultValue,
			Class<?> aType) {
		key = aKey;
		description = aDesc;
		defaultValue = aDefaultValue;
		type = aType;
	}
	
	public Optional<String> getOptVal(Properties props, String... patternReplacements) {
		return Optional.fromNullable(getValue(props, patternReplacements));
	}
	
	public String getValue(Properties props, String... patternReplacements) {
		return props.getProperty(String.format(key, patternReplacements), defaultValue);
	}
	
	public String getKey(String... patternReplacements) {
		return String.format(key, patternReplacements);
	}
	
	public List<String> getList(Properties props) {
		String val = getValue(props);
		String[] vals = val.split(",");
		List<String> result = new ArrayList<String>();
		for (String v: vals) {
			result.add(v.trim());
		}
		return ImmutableList.copyOf(result);
	}
	
	public Boolean getBoolValue(Properties props, String... patternReplacements) {
		return Boolean.valueOf(getValue(props, patternReplacements));
	}
	
	public Integer getIntValue(Properties props, String... patternReplacements) {
		return Integer.valueOf(getValue(props, patternReplacements));
	}

	public Optional<Integer> getOptIntVal(Properties props, String... patternReplacements) {
		Optional<String> optVal = getOptVal(props, patternReplacements);
		if (optVal.isPresent()) {
			return Optional.of(Integer.valueOf(optVal.get()));
		} else return Optional.absent();
	}

	public Optional<Long> getOptLongVal(Properties props, String... patternReplacements) {
		Optional<String> optVal = getOptVal(props, patternReplacements);
		if (optVal.isPresent()) {
			return Optional.of(Long.valueOf(optVal.get()));
		} else return Optional.absent();
	}
	
}
