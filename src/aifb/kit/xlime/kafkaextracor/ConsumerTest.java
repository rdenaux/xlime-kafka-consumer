package aifb.kit.xlime.kafkaextracor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;

public class ConsumerTest implements Runnable {
	private KafkaStream stream;
	private int threadNumber;
	private String kafkaTopic;

	public ConsumerTest(KafkaStream a_stream, int a_threadNumber, String mytopic) {

		threadNumber = a_threadNumber;
		stream = a_stream;
		kafkaTopic = mytopic;

	}

	public void run() {
		System.out.println("calling ConsumerTest.run()");

		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		int countConsumed = 0;
		int countProcessedCorrectly = 0;

		while (it.hasNext()) {
			if (countConsumed == 10)
				break;
			MessageAndMetadata<byte[], byte[]> mm = it.next();
			countProcessedCorrectly += process(mm, countConsumed++);
		}

		System.out.println("Shutting down Thread: " + threadNumber + " consumed " 
				+ countConsumed + " messages, and processed " + countProcessedCorrectly + " of those correctly.");
	}

	private int process(MessageAndMetadata<byte[], byte[]> mm, final int count) {
		int result = 0; //by default assume message will not be processed correctly
		System.out.println("processing topic=" + mm.topic() + ", partition="
				+ mm.partition() + ", offset=" + mm.offset());
		// System.out.println(message);

		Dataset dataset = DatasetFactory.createMem();
		try {
			final byte[] bytes = mm.message();
			System.out.println("message size (bytes): " + bytes.length);
			String message = new String(bytes, StandardCharsets.UTF_8);
			InputStream stream = new ByteArrayInputStream(
					message.getBytes(StandardCharsets.UTF_8));
			RDFDataMgr.read(dataset, stream, Lang.TRIG);
			Iterator names = dataset.listNames();
			System.out.println("\tNamed graphs in dataset: ");
			while (names.hasNext()) {
				String name = (String) names.next();
				System.out.println("\t\t"+ name);
				Model model = dataset.getNamedModel(name);
				System.out.println("\t\t\t" + model.size() + " triples");
			}

			if (dataset.getDefaultModel() != null
					&& dataset.getDefaultModel().size() > 0) {
				tryToStoreLocallyAsJSONLD(count, dataset);
			} else {
				System.out.println("\t\tno (or empty) default model.");
			}
			result = 1; //success
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error processing message " + e.getMessage());
		} finally {
			dataset.close();
		}
		return result; 
	}

	private void tryToStoreLocallyAsJSONLD(int count, Dataset dataset) {
		try {
			File file = new File("target/storedata/" + kafkaTopic
					+ "/topicfile" + count + ".json");
			Files.createParentDirs(file);
			OutputStream output = new FileOutputStream(file);
			RDFDataMgr.write(output, dataset, Lang.JSONLD);
			System.out.println("Wrote file " + file.getAbsolutePath());
			// m.write(output);
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Error storing dataset locally as json");
		}
	}
}