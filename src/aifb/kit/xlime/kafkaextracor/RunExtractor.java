package aifb.kit.xlime.kafkaextracor;

public class RunExtractor {

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: RunExtractor [topicName] [numOfPartitionsInTopic] [secondsToRun]");
			System.exit(0);
		}
		 //String zooKeeper = args[0];
        String zooKeeper = "aifb-ls3-hebe.aifb.kit.edu:2181";
        // String groupId = args[1];
        String groupId = "ISOCOTopicCaching-test1";
        String topic = args[0];
        int threads = Integer.parseInt(args[1]);
        int secsToRun = 10;
        if (args.length > 2) secsToRun = Integer.parseInt(args[2]);
        
        ExtractKafkaData example = new ExtractKafkaData(zooKeeper, groupId, topic);
        example.run(threads);
 
        try {
            Thread.sleep(secsToRun * 1000);
        } catch (InterruptedException ie) {
        	ie.printStackTrace();
        	System.err.println("");
        }
        example.shutdown();
	}

}
