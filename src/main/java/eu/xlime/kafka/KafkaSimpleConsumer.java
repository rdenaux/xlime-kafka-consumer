package eu.xlime.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
/**
 * Proof of concept to check Kafka's SimpleConsumer, which is actually a more complicated way of consuming
 * Kafka streams. It's often better to use the High-level consumer, such as {@link KafkaStreamConsumerLauncher}.
 * 
 * @author RDENAUX
 *
 */
public class KafkaSimpleConsumer {
	/**
	 * Tries to read the last <code>maxReads</code> messages from a <code>topic</code> using a particular <code>partition</code>, 
	 * although it only reads at most 'maxRead' times.  
	 * @param args
	 */
    public static void main(String args[]) {
    	if (args.length < 5 || args.length > 6) {
    		System.out.println("Usage: KafkaSimpleConsumer [maxReads] [topic] [partition] [seed] [port] [rewind]");
    		System.exit(1);
    	}
        KafkaSimpleConsumer example = new KafkaSimpleConsumer();
        long maxReads = Long.parseLong(args[0]);
        String topic = args[1];
        int partition = Integer.parseInt(args[2]);
        List<String> seeds = new ArrayList<String>();
        seeds.add(args[3]);
        int port = Integer.parseInt(args[4]);
        
        int rewind = 0;
        if (args.length > 5) rewind = Integer.parseInt(args[5]);
        
        try {
            example.run(maxReads, topic, partition, seeds, port, rewind);
        } catch (Exception e) {
            System.out.println("Oops:" + e);
             e.printStackTrace();
        }
    }
 
    private List<String> m_replicaBrokers = new ArrayList<String>();
 
    public KafkaSimpleConsumer() {
        m_replicaBrokers = new ArrayList<String>();
    }
 
    /**
     * 
     * @param a_maxReads how many messages to process
     * @param a_topic the name of the kafka topic to process
     * @param a_partition the number of the partition of the kafka topic to process
     * @param a_seedBrokers the domain name of the zookeeper broker
     * @param a_port the port of the broker (typically 9092)
     * @param rewind the number of messages to rewind when starting again (use 0 to never rewind)
     * @throws Exception
     */
    public void run(final long a_maxReads, final String a_topic, final int a_partition, final List<String> a_seedBrokers, final int a_port, final int rewind) throws Exception {
        // find the meta data about the topic and partition we are interested in
        //
    	System.out.println("Finding leader");
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client1_" + a_topic + "_" + a_partition;
 
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

        System.out.println("Earliest time offset for " + a_topic + ":" + a_partition 
        		+ " for " + clientName + " = " + readOffset + " at time " + kafka.api.OffsetRequest.EarliestTime());
        
        int numErrors = 0;
        long messagesLeft = a_maxReads;
        while (messagesLeft > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
            }
            long reqOffset = Math.max(0, readOffset - rewind);
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(a_topic, a_partition, reqOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            System.out.println("Requesting offset " + reqOffset);
            FetchResponse fetchResponse = consumer.fetch(req);
 
            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                	System.out.println("Offset out of range");
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    System.out.println("Latest time offset for " + a_topic + ":" 
                    + a_partition + " for " + clientName + " = " + readOffset + " at time " + kafka.api.OffsetRequest.LatestTime());
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                continue;
            }
            numErrors = 0;
 
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("(Re)processing an old offset: " + currentOffset + " should have started from: " + readOffset);
//                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                System.out.println("Reading " + currentOffset + ", set readOffset to " + readOffset);
                ByteBuffer payload = messageAndOffset.message().payload();
 
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(currentOffset) + ": " + new String(bytes, "UTF-8"));
                numRead++;
                messagesLeft--;
            }
 
            if (numRead == 0) {
                try {
                	System.out.println("Didn't read any messages, but still configured to read " + messagesLeft + ". Waiting...");
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null) consumer.close();
    }
 
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 10));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
 
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < offsets.length; i++) {
        	sb.append(offsets[i]).append(", ");
        }
        System.out.println("Offsets for " + topic + ":" + partition + " = " + sb.toString());
        return offsets[0];
    }
 
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
 
    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                System.out.println("Requesting topic metadata for " + topics + "...");
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                System.out.println("Received topic metadata");
 
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
}
