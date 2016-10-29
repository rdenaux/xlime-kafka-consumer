package eu.xlime.kafka.msgproc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;

import eu.xlime.kafka.KafkaMessageAndMetadataProcessor;
import kafka.message.MessageAndMetadata;

/**
 * Special type of {@link KafkaMessageAndMetadataProcessor} which attempts to parse the 
 * Kafka message as RDF (assuming a particular RDF {@link #lang}uage) and delegates
 * further processing to a {@link DatasetProcessor}.
 *  
 * @author rdenaux
 *
 */
public class RDFMessageParser implements KafkaMessageParser<Dataset> {

	private static final Logger log = LoggerFactory.getLogger(RDFMessageParser.class);
	
	/**
	 * The assumed RDF {@link Lang}uage contained by the Kafka messages to be processed.
	 */
	private final Lang lang;
	
	/**
	 * Delegate processor which will handle the parsed RDF {@link Dataset}s.
	 */
	private final DatasetProcessor datasetProcessor;
	
	public RDFMessageParser(Lang lang, DatasetProcessor datasetProcessor) {
		super();
		this.lang = lang;
		this.datasetProcessor = datasetProcessor;
	}

	public boolean process(MessageAndMetadata<byte[], byte[]> mm) {
		Optional<Dataset> optDs = Optional.absent();
		try {
			optDs = parse(mm);
			if (optDs.isPresent()) return processDataset(mm, optDs.get());
			else return false;
		} catch (Exception e) {
			log.error("Error parsing or processing parsed dat", e);
		} finally {
			if (optDs.isPresent()) optDs.get().close();
		}
		return false;
	}

	/**
	 * Returns an optional, <b>open</b> (thus queryable) {@link Dataset} with the 
	 * result of parsing the message in <code>mm</code> 
	 */
	public Optional<Dataset> parse(MessageAndMetadata<byte[], byte[]> mm) {
		try {
			return parse(mm.message());
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error processing message " + e.getMessage());
		} 
		return Optional.absent();
	}

	private Optional<Dataset> parse(byte[] bytes) {
		Dataset dataset = DatasetFactory.createMem();
		try {
			InputStream stream = new ByteArrayInputStream(
					bytes);
			RDFDataMgr.read(dataset, stream, lang);
		} catch (Exception e) {
			log.error("Failed to parse RDF from input stream", e);
			dataset.close();
			return Optional.absent();
		}
		if (log.isTraceEnabled()) {
			log.trace(summariseNamedGraphs(dataset));
		}
		return Optional.of(dataset);
	}

	private boolean processDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset) {
		return datasetProcessor.processDataset(mm, dataset);
	}

	private String summariseNamedGraphs(Dataset dataset) {
		StringBuilder sb = new StringBuilder();
		Iterator<String> names = dataset.listNames();
		sb.append("Named graphs in dataset: \n");
		while (names.hasNext()) {
			String name = (String) names.next();
			Model model = dataset.getNamedModel(name);
			sb.append(String.format("\t%s %s triples", name, model.size()));
		}
		return sb.toString();
	}
	
}
