package eu.xlime.kafka;

public enum XLiMeKafkaTopic {

	socialmedia,
	zattoo_epg;
	
	public static XLiMeKafkaTopic fromTopicName(String topicName) {
		return valueOf(topicName.replaceAll("-", "_"));
	}
}
