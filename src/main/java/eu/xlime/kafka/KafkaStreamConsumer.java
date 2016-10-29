package eu.xlime.kafka;

import java.util.Properties;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * {@link Runnable} able to consume xLiMe {@link KafkaStream}s. It {@link #process(MessageAndMetadata, long)}es a fixed number 
 * of messages from a {@link KafkaStream}.
 * 
 * @author rdenaux
 *
 */
public class KafkaStreamConsumer implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(KafkaStreamConsumer.class);
	
	private final Properties cfgProps;
	private final KafkaStream<byte[], byte[]> stream;
	private final int threadNumber;
	private final String kafkaTopic;
	private final KafkaMessageAndMetadataProcessor msgProcessor;
	private final Optional<Long> maxCountConsumed;

	public KafkaStreamConsumer(Properties props, KafkaStream a_stream, int a_threadNumber, String mytopic) {
		cfgProps = props;
		threadNumber = a_threadNumber;
		stream = a_stream;
		kafkaTopic = mytopic;
		msgProcessor = new XLiMeKafkaMessageProcessor(props); //TODO: make configurable (via config, guice, other?)
		maxCountConsumed = readMaxCountConsumed();
	}

	private Optional<Long> readMaxCountConsumed() {
		return ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_MAX_COUNT_CONSUMED.getOptLongVal(cfgProps, kafkaTopic);
	}

	public void run() {
		log.info(String.format("%s for %s_%s", "KafkaStreamConsumer.run()", kafkaTopic, threadNumber));
		try {
			Thread.currentThread().setName(String.format("kafka-stream-%s-consumer-%s", kafkaTopic, threadNumber));
		} catch (Exception e) {
			log.warn("Failed to rename thread by " + getClass().getSimpleName());
		}

		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		long countConsumed = 0;
		long countProcessedCorrectly = 0;

		while (it.hasNext()) {
			if (maxCountConsumed.isPresent() && countConsumed >= maxCountConsumed.get()) {
				log.info("Reached maximum count consumed");
				break;
			}
			MessageAndMetadata<byte[], byte[]> mm = it.next();
			countConsumed++;
			try {
				if (msgProcessor.process(mm)) countProcessedCorrectly++;
			} catch (Throwable t) {
				log.error("Error processing message", t);
			}
		}

		if (!it.hasNext()) {
			log.info("Reached end of stream, shutting down stream consumer thread (should wait instead?).");
		}
		
		log.info(String.format("Shutting down Thread: %s consumed %s messages, and processed %s of those correctly", threadNumber,
				countConsumed, countProcessedCorrectly));
	}


}