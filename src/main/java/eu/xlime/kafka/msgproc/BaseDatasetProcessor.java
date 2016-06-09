package eu.xlime.kafka.msgproc;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndMetadata;

import com.google.common.base.Optional;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.kafka.ConfigOptions;

/**
 * Provides some basic logging and statistics keeping for {@link DatasetProcessor}s.
 * 
 * @author rdenaux
 *
 */
public abstract class BaseDatasetProcessor implements DatasetProcessor {

	private static final Logger log = LoggerFactory.getLogger(BaseDatasetProcessor.class);
	
	private static int instCount = 0;
	protected final int instId;
	private long summariseEvery;
	private long messagesProcessed = 0;

	protected BaseDatasetProcessor(Properties props) {
		instId = instCount++;
		Optional<Long> optVal = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_RDF_DATASET_PROCESSOR_SUMMARISE_EVERY.getOptLongVal(props);
		summariseEvery = optVal.or(500L);
	}
	
	public final boolean processDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset) {
		messagesProcessed++;
		trySummarise();
		try {
			return doProcessDataset(mm, dataset);
		} catch (Exception e) {
			log.error("Failed to process dataset", e);
			return false;
		}
	}
	
	/**
	 * Child classes should implement this method to perform the {@link #processDataset(MessageAndMetadata, Dataset)}.
	 *  
	 * @param mm
	 * @param dataset
	 * @return
	 */
	abstract protected boolean doProcessDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset);

	private void trySummarise() {
		if (summariseEvery <= 0) return;
		try {
			if (messagesProcessed % summariseEvery == 0) {
				log.info(generateSummary());
			}
		} catch (Exception e) {
			log.warn("Failed to log summary", e);
		}
	}
	
	protected final long getMessagesProcessed() {
		return messagesProcessed;
	}

	/**
	 * Generates the summary of the processing done thus far by this {@link DatasetProcessor}. We recommend 
	 * subclasses to re-implement this method to give more detailed information while using the {@link #instId} and
	 * {@link #getMessagesProcessed()} information provided by the {@link BaseDatasetProcessor}.
	 * 
	 * @return
	 */
	protected String generateSummary() {
		return String.format("%s_%s processed=%s", getClass().getSimpleName(), instId, messagesProcessed);
	}

}
