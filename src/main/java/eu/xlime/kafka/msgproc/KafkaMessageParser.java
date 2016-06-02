package eu.xlime.kafka.msgproc;

import com.google.common.base.Optional;

import eu.xlime.kafka.KafkaMessageAndMetadataProcessor;
import kafka.message.MessageAndMetadata;

/**
 * Special type of {@link KafkaMessageAndMetadataProcessor} which tries to parse
 * the Kafka message and provide a parsed result object.
 *  
 * @author rdenaux
 *
 * @param <T> the type of the parsed result
 */
public interface KafkaMessageParser<T> extends KafkaMessageAndMetadataProcessor {

	/**
	 * Returns the parsed result for <code>mm</code>
	 * @param mm
	 * @return
	 */
	Optional<T> parse(MessageAndMetadata<byte[], byte[]> mm);

}
