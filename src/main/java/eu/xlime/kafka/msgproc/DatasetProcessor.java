package eu.xlime.kafka.msgproc;

import java.util.Properties;

import kafka.message.MessageAndMetadata;

import com.hp.hpl.jena.query.Dataset;

/**
 * Interface for DatasetProcessor. Implementing classes must also 
 * implement a constructor with a single {@link Properties} parameter to enable 
 * the {@link DatasetProcessorFactory} to instantiate it.
 * 
 * @author rdenaux
 *
 */
public interface DatasetProcessor {

	/**
	 * Tries to process the dataset (within the context of the Kafka {@link MessageAndMetadata}) 
	 * and notifies whether the process was successfull.
	 * @param mm provides the context for the {@link Dataset}. Typically, the {@link Dataset} was parsed from the message part. 
	 * @param dataset the {@link Dataset} that needs to be processed.
	 * @return <code>true</code> if the processing could be performed correctly, <code>false</code>
	 *  otherwise.
	 */
	boolean processDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset);
	
}
