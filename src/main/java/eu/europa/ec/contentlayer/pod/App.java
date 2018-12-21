package eu.europa.ec.contentlayer.pod;

import java.util.*;

import eu.europa.ec.contentlayer.pod.pojo.ConsumerService;
import eu.europa.ec.contentlayer.pod.pojo.ProducerService;
import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdfconnection.RDFConnectionFuseki;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;


import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import eu.europa.ec.contentlayer.pod.consumer.ConsumerCreator;
import eu.europa.ec.contentlayer.pod.producer.ProducerCreator;

public class App {
	public static void main(String[] args) {

		runProducer();
		System.out.println("Data send");
		runConsumer();
		System.out.println("Data consumed");
	}

	static void runConsumer() {
		Consumer<String, RdfTransaction> consumer = ConsumerCreator.createConsumer();

        RDFConnectionRemoteBuilder tripleStoreConnection = RDFConnectionFuseki.create()
                .destination("http://localhost:3030/datapod/query")
                .updateEndpoint("http://localhost:3030/datapod/update");

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<String, RdfTransaction> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
			}

			consumerRecords.forEach(record -> {
				Model model = ConsumerService.transactionToSubjectModel(record);
				String graphName = "http://" + record.topic() + "/";
				String insert = ConsumerService.subjectModelToInsertQuery(model, graphName);

				UpdateRequest query = UpdateFactory.create(insert);
				try {
					tripleStoreConnection.build().update(query);
				}
				catch (Exception e) {
					System.out.println("Error inserting triples");
				}
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	static void runProducer() {
		Producer<String, RdfTransaction> producer = ProducerCreator.createProducer();

		Model model = ProducerService.buildModelFromFile("/home/sander/eurovoc.rdf");

		Map<String, Model> subjectModels = ProducerService.extractSubjectModels(model);
		System.out.println("Model parsed");
		subjectModels.forEach((subject, subjectModel) -> {
			RdfTransaction Transaction = ProducerService.buildRdfTransaction(subjectModel);
			ProducerService.commitTransaction(producer, Transaction, subject);
		});
	}

}
