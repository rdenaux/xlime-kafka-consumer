package eu.xlime.kafka.rdf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.kafka.msgproc.DatasetProcessor;

/**
 * {@link DatasetProcessor} which stores the input {@link Dataset}s to the local disc
 * as Json-ld files.
 * 
 * @author rdenaux
 *
 */
public class JsonLDStorer implements DatasetProcessor {
	
	private static final Logger log = LoggerFactory.getLogger(JsonLDStorer.class);

	public JsonLDStorer(Properties cfgProps) {
		
	}
	
	public boolean processDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset) {
		
		if (dataset.getDefaultModel() != null
				&& dataset.getDefaultModel().size() > 0) {
			return tryToStoreLocallyAsJSONLD(dataset, mm);
		} else {
			log.info("\t\tno (or empty) default model.");
			return true;
		}
	}

	private boolean tryToStoreLocallyAsJSONLD(Dataset dataset, MessageAndMetadata mm) {
		try {
			String fname = String.format("target/storedata/%s/p%s_o%s.json", mm.topic(), mm.partition(), mm.offset());
			File file = new File(fname);
			Files.createParentDirs(file);
			OutputStream output = new FileOutputStream(file);
			RDFDataMgr.write(output, dataset, Lang.JSONLD);
			log.info("Wrote file " + file.getAbsolutePath());
			output.close();
			return true;
		} catch (IOException e) {
			log.error("Error storing dataset locally as json", e);
			return false;
		}
	}
	
}
