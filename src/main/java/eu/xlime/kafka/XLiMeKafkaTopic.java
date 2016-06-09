package eu.xlime.kafka;

public enum XLiMeKafkaTopic {

	socialmedia,
	jsi_newsfeed,
	zattoo_asr,
	tv_ocr,
	zattoo_sub,
	zattoo_epg;
	
	public static XLiMeKafkaTopic fromTopicName(String topicName) {
		return valueOf(topicName.replaceAll("-", "_"));
	}
}
