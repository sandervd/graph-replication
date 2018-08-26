package eu.europa.ec.contentlayer.pod;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;

import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.FileManager;
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
		Consumer<String, RdfTransaction> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<String, RdfTransaction> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
			});
			consumer.commitAsync();

		}
		consumer.close();
	}

	static void runProducer() {
		Producer<String, RdfTransaction> producer = ProducerCreator.createProducer();

		Model model = getModel();

		ResIterator Subjects = model.listSubjects();
		while (Subjects.hasNext()) {
			Resource subject = Subjects.nextResource();
			RdfTransaction Transaction = buildRdfTransaction(subject);
			commitTransaction(producer, Transaction, subject.toString());
		}
	}

	private static RdfTransaction buildRdfTransaction(Resource subject) {
		RdfTransaction Transaction = new RdfTransaction();

		Transaction.setStatements(serializeStatements(subject));

		return Transaction;
	}

	private static Model getModel() {
		String inputFileName = "/home/sander/eurovoc.rdf";

		// create an empty model
		Model model = ModelFactory.createDefaultModel();

		// use the FileManager to find the input file
		InputStream in = FileManager.get().open( inputFileName );
		if (in == null) {
			throw new IllegalArgumentException(
					"File: " + inputFileName + " not found");
		}

		// read the RDF/XML file
		model.read(in, null);
		return model;
	}

	private static String serializeStatements(Resource subject) {
		StmtIterator Properties = subject.listProperties();
		OutputStream out = new ByteArrayOutputStream();
		Properties.toModel().write(out);

		return out.toString();
	}

	private static void commitTransaction(Producer<String, RdfTransaction> producer, RdfTransaction transaction, String Subject) {
		final ProducerRecord<String, RdfTransaction> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, null, Subject, transaction);
		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out.println("Record sent to partition " + metadata.partition()
					+ " with offset " + metadata.offset());
		}
		// @todo Rethrow exception to cancel triplestore transaction.
		catch (ExecutionException |InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}
	}
}
