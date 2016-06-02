package eu.xlime.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigFactory {

	private static final Logger log = LoggerFactory.getLogger(ConfigFactory.class);
	
	public static Properties loadPropertiesFromPath(String propertiesPath) {
		Properties result = new Properties();
		try {
			log.info("Loading configuration from " + propertiesPath);			
			File f = new File(propertiesPath);
			log.info("Configuration resolves to " + f.getAbsolutePath() + " which exists? " + f.exists());
			result.load(new FileInputStream(f));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("In order to correctly load configurations, you "
					+ "need to create a properties file "
					+ "with valid settings. You can use "
					+ "src/main/resources/xlime-kafka-consumer-sample-config.properties "
					+ "as a starting point.", e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return result;
	}
	
}
