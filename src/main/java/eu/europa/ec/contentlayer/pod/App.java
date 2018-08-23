package eu.europa.ec.contentlayer.pod;

import java.util.concurrent.ExecutionException;

import eu.europa.ec.contentlayer.pod.pojo.CustomObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import eu.europa.ec.contentlayer.pod.consumer.ConsumerCreator;
import eu.europa.ec.contentlayer.pod.producer.ProducerCreator;

public class App {
	public static void main(String[] args) {
		runProducer();
		runConsumer();
	}

	static void runConsumer() {
		Consumer<Long, CustomObject> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, CustomObject> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				//System.out.println("Record Key " + record.key());
				//System.out.println("Record value - name: " + record.value().getName());
				System.out.println("Record value - id: " + record.value().getId());
				//System.out.println("Record partition " + record.partition());
				//System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	static void runProducer() {
		Producer<Long, CustomObject> producer = ProducerCreator.createProducer();

		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			CustomObject test = new CustomObject();
			test.setName("My names is " + index);
			test.setId(String.valueOf(index));
			final ProducerRecord<Long, CustomObject> record = new ProducerRecord<Long, CustomObject>(IKafkaConstants.TOPIC_NAME, test);
			try {
				RecordMetadata metadata = producer.send(record).get();
				//System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
				//		+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}
}
