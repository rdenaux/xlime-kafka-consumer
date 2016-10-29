package eu.xlime.kafka;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import org.apache.jena.riot.Lang;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import eu.xlime.kafka.msgproc.DatasetProcessor;
import eu.xlime.kafka.msgproc.DatasetProcessorFactory;
import eu.xlime.kafka.msgproc.RDFMessageParser;

/**
 * {@link KafkaMessageAndMetadataProcessor} which delegates the processing to one of the 
 * known processors for supported {@link XLiMeKafkaTopic}s.
 *  
 * @author rdenaux
 *
 */
public class XLiMeKafkaMessageProcessor implements KafkaMessageAndMetadataProcessor {
	private static final Logger log = LoggerFactory.getLogger(XLiMeKafkaMessageProcessor.class);

	private final Properties cfgProps;

	private final Map<XLiMeKafkaTopic, KafkaMessageAndMetadataProcessor> topicProcessors;
	
	public XLiMeKafkaMessageProcessor(Properties cfgProps) {
		super();
		this.cfgProps = cfgProps;
		topicProcessors = buildTopicProcessor();
	}

	private Map<XLiMeKafkaTopic, KafkaMessageAndMetadataProcessor> buildTopicProcessor() {
		Map<XLiMeKafkaTopic, KafkaMessageAndMetadataProcessor> result = new HashMap<XLiMeKafkaTopic, KafkaMessageAndMetadataProcessor>();
		List<String> topicNames = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPICS.getList(cfgProps);
		for (String topicName: topicNames) {
			XLiMeKafkaTopic topic = XLiMeKafkaTopic.fromTopicName(topicName);
			KafkaMessageAndMetadataProcessor processor = buildMessageProcessor(topic, topicName);
			result.put(topic, processor);
		}
		return ImmutableMap.copyOf(result);
	}

	public boolean process(MessageAndMetadata<byte[], byte[]> mm) {
		boolean result = false; //by default assume message will not be processed correctly
		try {
			tryLogMessage(mm);
			return delegateProcess(mm);
		} catch (Exception e) {
			log.error("Failed to process message", e);
		}
		return result;
	}

	private boolean delegateProcess(MessageAndMetadata<byte[], byte[]> mm) {
		XLiMeKafkaTopic topic = XLiMeKafkaTopic.fromTopicName(mm.topic());
		Optional<KafkaMessageAndMetadataProcessor> optDelegate = getDelegateProcessor(topic);
		if (optDelegate.isPresent()) return optDelegate.get().process(mm);
		else {
			log.error("Could not find a message processor for topic " + mm.topic());
			return false;
		}
	}

	private void tryLogMessage(MessageAndMetadata<byte[], byte[]> mm) {
		if (log.isTraceEnabled()) {
			log.trace(String.format("processing topic=%s, partition=%s, offset=%s", mm.topic(), mm.partition(), mm.offset()));
		}
		try {
			final byte[] bytes = mm.message();
			if (log.isTraceEnabled()) {
				log.trace("message size (bytes): " + bytes.length);
			}
			if (log.isTraceEnabled()) { 
				String message = new String(bytes, StandardCharsets.UTF_8);
				log.trace(message); 
			}
		} catch (Exception e) {
			log.error("Failed to log message", e);
		}
	}

	private Optional<KafkaMessageAndMetadataProcessor> getDelegateProcessor(
			XLiMeKafkaTopic topic) {
		if (topic == null) return Optional.absent();
		return Optional.fromNullable(doGetMessageProcessor(topic));
	}

	private KafkaMessageAndMetadataProcessor doGetMessageProcessor(
			XLiMeKafkaTopic topic) {
		if (topicProcessors.containsKey(topic)) 
			return topicProcessors.get(topic);
		else return null;
	}

	private KafkaMessageAndMetadataProcessor buildMessageProcessor(
			XLiMeKafkaTopic topic, String topicName) {
		switch(topic) {
		case socialmedia: return new RDFMessageParser(Lang.TRIG, buildDatasetProcessor(topicName));
		case zattoo_epg: return new RDFMessageParser(Lang.TRIG, buildDatasetProcessor(topicName));
		case jsi_newsfeed: return new RDFMessageParser(Lang.TRIG, buildDatasetProcessor(topicName));
		case tv_ocr: return new RDFMessageParser(Lang.TRIG, buildDatasetProcessor(topicName));
		case zattoo_asr: return new RDFMessageParser(Lang.TRIG, buildDatasetProcessor(topicName));
		case zattoo_sub: return new RDFMessageParser(Lang.TRIG, buildDatasetProcessor(topicName));
		default: return null;
		}
	}
	
	private DatasetProcessor buildDatasetProcessor(
			String topicName) {
		DatasetProcessorFactory factory = new DatasetProcessorFactory(getClass().getClassLoader());
		ConfigOptions opt = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_RDF_DATASET_PROCESSOR;
		Optional<String> optFQN = opt.getOptVal(cfgProps, topicName);
		if (!optFQN.isPresent()) throw new RuntimeException("You must specify a configuration option " + opt.getKey(topicName));
		Optional<DatasetProcessor> optDSP = factory.createInstance(optFQN.get(), cfgProps);
		if (!optDSP.isPresent()) throw new RuntimeException("Failed to create a DatasetProcessor for " + optFQN + ". Check logs and configuration.");
		return optDSP.get();
	}	
	
}
