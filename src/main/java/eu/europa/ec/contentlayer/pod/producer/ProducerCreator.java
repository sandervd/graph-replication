package eu.europa.ec.contentlayer.pod.producer;

import java.util.Properties;

import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;
import eu.europa.ec.contentlayer.pod.serializer.RdfTransactionSerializer;
import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerCreator {

	public static Producer<String, RdfTransaction> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RdfTransactionSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
}