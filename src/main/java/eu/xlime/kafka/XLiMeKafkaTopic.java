package eu.xlime.kafka;

public enum XLiMeKafkaTopic {

	socialmedia,
	jsi_newsfeed,
	zattoo_epg;
	
	public static XLiMeKafkaTopic fromTopicName(String topicName) {
		return valueOf(topicName.replaceAll("-", "_"));
	}
}
