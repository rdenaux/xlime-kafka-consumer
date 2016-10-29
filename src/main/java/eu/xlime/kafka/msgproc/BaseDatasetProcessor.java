package eu.xlime.kafka.msgproc;

import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndMetadata;

import com.google.common.base.Optional;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;

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
	protected final Date processorCreationDate; 
	private long summariseEveryNMessages;
	private long summariseEveryNMinutes;
	private long bytesProcessed = 0; 
	private long messagesProcessed = 0;
	private long messagesFailedToProcess = 0;
	private long nullDatasets = 0;
	private long errorsCountingRDFMetrics = 0;
	private long namedGraphs = 0;
	private long quads = 0;
	private long datasetProcessingTimeInMs = 0;
	private boolean needsTimedSummary = true;
	
	protected BaseDatasetProcessor(Properties props) {
		instId = instCount++;
		processorCreationDate = new Date();
		Optional<Long> optVal = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_RDF_DATASET_PROCESSOR_SUMMARISE_EVERY_N_MESSAGES.getOptLongVal(props);
		summariseEveryNMessages = optVal.or(500L);
		
		Optional<Long> optVal2 = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_RDF_DATASET_PROCESSOR_SUMMARISE_EVERY_N_MESSAGES.getOptLongVal(props);
		summariseEveryNMinutes = optVal2.or(10L);
		
		if (summariseEveryNMinutes > 0) {
			Timer timer = new Timer();
			timer.schedule(tryLogTimedSummaryTask(), 5 * 1000, summariseEveryNMinutes * 60 * 1000);
		}
	}
	
	private TimerTask tryLogTimedSummaryTask() {
		return new TimerTask() {

			@Override
			public void run() {
				if (needsTimedSummary) {
					try {
						performSummarisation();
					} catch (Exception e) {
						log.error("Failed to perform timed summarisation", e);
					}
				} else {
					log.info(String.format("Skipping timed summary for %s_%s (assuming n-message summary has been triggered)", getClass().getSimpleName(), instId));
				}
				needsTimedSummary = true;
			}
			
		};
	}

	public final boolean processDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset) {
		messagesProcessed++;
		tryUpdateBytesProcessed(mm);
		tryUpdateDatasetCounters(dataset);
		boolean result = false;
		try {
			long start = System.currentTimeMillis();
			result = doProcessDataset(mm, dataset);
			long end = System.currentTimeMillis();
			datasetProcessingTimeInMs = datasetProcessingTimeInMs + (end - start);
		} catch (Exception e) {
			log.error("Failed to process dataset", e);
			messagesFailedToProcess++;
			result = false;
		}
		tryNMessageSummarisation();
		return result;
	}

	private void tryUpdateBytesProcessed(MessageAndMetadata<byte[], byte[]> mm) {
		if (mm == null) return;
		try {
			try {
				bytesProcessed = bytesProcessed + mm.message().length;
			} catch (NullPointerException e) {
				bytesProcessed = bytesProcessed + mm.rawMessage$1().size();
			}
		} catch (Exception e) {
			log.error("Error counting bytes processed", e);
		}
	}
	
	private void tryUpdateDatasetCounters(Dataset dataset) {
		try {
			if (dataset == null) {
				nullDatasets++;
				return;
			}
			DatasetGraph dsg = dataset.asDatasetGraph();
			long dsgSize = dsg.size();
			if (dsgSize > 0) {
				namedGraphs = namedGraphs + dsg.size();
			}
			quads = quads + countQuads(dsg);
		} catch (Exception e) {
			log.error("Error trying to update dataset counter", e);
			errorsCountingRDFMetrics++;
		}
	}

	private long countQuads(DatasetGraph dsg) {
		long result = 0;
		final Iterator<Quad> it = dsg.find();
		while (it.hasNext()) {
			it.next();
			result++;
		}
		return result;
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

	private void tryNMessageSummarisation() {
		if (summariseEveryNMessages <= 0) return;
		try {
			if (messagesProcessed % summariseEveryNMessages == 0) {
				performSummarisation();
				needsTimedSummary = false;
			}
		} catch (Exception e) {
			log.warn("Failed to generate, log or process summary", e);
		}
	}

	private void performSummarisation() {
		Object summary = generateSummary();
		log.info(generateSummaryString(summary));
		processSummaryObject(summary);
	}
	
	
	protected final long getBytesProcessed() {
		return bytesProcessed;
	}

	protected final long getMessagesProcessed() {
		return messagesProcessed;
	}
	
	protected final long getMessagesFailedToProcess() {
		return messagesFailedToProcess;
	}

	protected final long getSeenNullDatasets() {
		return nullDatasets;
	}

	protected final long getSeenNamedGraphs() {
		return namedGraphs;
	}

	protected final long getSeenRDFQuads() {
		return quads;
	}

	protected final long getErrorsCountingRDFMetrics() {
		return errorsCountingRDFMetrics;
	}

	protected final long getDatasetProcessingTimeInMs() {
		return datasetProcessingTimeInMs;
	}

	/**
	 * Generates the summary object of the processing done thus far by this {@link DatasetProcessor}. We recommend 
	 * subclasses to re-implement this method to return a custom summary.
	 * 
	 * We recommend subclasses to include information provided by the {@link BaseDatasetProcessor} such as the 
	 * {@link #instId} and additional information provided by the {@link BaseDatasetProcessor}:
	 * 
	 * See {@link #getMessagesProcessed()}, {@link #getMessagesFailedToProcess()}, 
	 * {@link #getSeenNullDatasets()}, {@link #getSeenNamedGraphs()}, 
	 * {@link #getSeenRDFQuads()} and {@link #getErrorsCountingRDFMetrics()}
	 * 
	 * @return a non-null summarisation object
	 */
	protected abstract Object generateSummary();
	
	/**
	 * Hook method for subclasses to perform custom processing for a generated summary, e.g. storing to disk or similar. 
	 * 
	 * @param summary
	 */
	protected void processSummaryObject(Object summary) {
		// nothing to do here
	}
	
	/**
	 * Generates the summary of the processing done thus far by this {@link DatasetProcessor}. We recommend 
	 * subclasses to re-implement this method to give more detailed information while using 
	 * 
	 * @return
	 */
	protected String generateSummaryString(Object summary) {
		if (summary != null)
			return summary.toString();
		else return String.format("%s_%s processed=%s", getClass().getSimpleName(), instId, messagesProcessed);
	}

}
