package eu.xlime.kafka;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * Main launcher class for xLiMe Kafka consumers.
 *  
 * @author rdenaux
 *
 */
public class RunExtractor {

	private static final Logger log = LoggerFactory.getLogger(RunExtractor.class);
	
	/**
	 * Execute one or more xLiMe Kafka consumers based on an <i>xLiMe kafka consumer properties</i> configuration file.
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: RunExtractor [pathToProps]");
			System.exit(0);
		}
        String propsPath = args[0];
        
        log.info("Loading configuration from " + propsPath);
        Properties props = ConfigFactory.loadPropertiesFromPath(propsPath);
        KafkaStreamConsumerLauncher consumerLauncher = new KafkaStreamConsumerLauncher(props); //zooKeeper, groupId);
        log.info("Launching consumers ");
        consumerLauncher.launchStreamConsumers();
        scheduleConsumerMonitor(consumerLauncher);
        log.info("Launching side tasks");
        consumerLauncher.launchSideTasks();
        
    	Runtime.getRuntime().addShutdownHook(new ConsumerLauncherShutdownThread(consumerLauncher));// register clean shutdown hook

        Optional<Integer> optVal = ConfigOptions.XLIME_KAFKA_CONSUMER_TIMED_RUN_S.getOptIntVal(props);
        if (optVal.isPresent()) {
        	int secsToRun = optVal.get();
            log.info("Timed consumer execution, running for (seconds) " + secsToRun);

        	try {
        		Thread.sleep(secsToRun * 1000);
        	} catch (InterruptedException ie) {
        		ie.printStackTrace();
        		System.err.println("Error waiting until timed run expires" + ie.getLocalizedMessage());
        	}
        	
            log.info("Timed consumer execution: time's up");
            System.exit(0);
            log.info("Started shutdown");
        }  
	}

	private static void scheduleConsumerMonitor(
			KafkaStreamConsumerLauncher consumerLauncher) {
		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(consumerMonitor(consumerLauncher), 1, 1, TimeUnit.MINUTES);
	}

	private static Runnable consumerMonitor(
			final KafkaStreamConsumerLauncher consumerLauncher) {
		return new Runnable() {
			public void run() {
				consumerLauncher.monitorConsumers();
				if (consumerLauncher.canExit()) {
					log.info("All kafka stream consumers have stopped; exiting application.");
					System.exit(0);
				}
			}
		};
	}

	/**
	 * Thread to handle the controlled shutdown of a {@link KafkaStreamConsumerLauncher}.
	 * 
	 * @author rdenaux
	 *
	 */
	private static class ConsumerLauncherShutdownThread extends Thread {
		private final KafkaStreamConsumerLauncher consumerLauncher;

		public ConsumerLauncherShutdownThread(
				KafkaStreamConsumerLauncher consumerLauncher) {
			super();
			this.consumerLauncher = consumerLauncher;
		}

		@Override
		public void run() {
			super.run();
            log.info("Shutting down consumers");
            if (consumerLauncher != null)
            	consumerLauncher.shutdown();
            log.info("finishing?");
		}
	}
	
}
