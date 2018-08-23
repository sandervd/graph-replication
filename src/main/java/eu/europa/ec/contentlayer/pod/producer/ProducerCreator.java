package eu.europa.ec.contentlayer.pod.producer;

import java.util.Properties;

import eu.europa.ec.contentlayer.pod.pojo.CustomObject;
import eu.europa.ec.contentlayer.pod.serializer.CustomSerializer;
import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

public class ProducerCreator {

	public static Producer<Long, CustomObject> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
}