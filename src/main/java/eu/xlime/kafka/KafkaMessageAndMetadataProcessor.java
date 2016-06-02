package eu.xlime.kafka;

import kafka.message.MessageAndMetadata;

/**
 * Defines interface for processing a Kafka {@link MessageAndMetadata} (i.e. 
 * a message from a given topic and partition).
 * 
 * @author rdenaux
 *
 */
public interface KafkaMessageAndMetadataProcessor {

	/**
	 * Attempts to process a Kafka message and returns whether the process
	 * was successful.
	 *  
	 * @param mm
	 * @return <code>true</code> if this {@link KafkaMessageAndMetadataProcessor} successfully
	 * 	processed the message and <code>false</code> otherwise. 
	 */
	boolean process(MessageAndMetadata<byte[], byte[]> mm);
	
}
