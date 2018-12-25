package eu.europa.ec.contentlayer.pod;

import java.util.*;

import eu.europa.ec.contentlayer.pod.pojo.AppConfig;
import eu.europa.ec.contentlayer.pod.pojo.ConsumerThread;
import eu.europa.ec.contentlayer.pod.pojo.ProducerService;
import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;
import eu.europa.ec.contentlayer.pod.statemachine.StateMachine;
import org.apache.jena.rdf.model.*;
import org.apache.kafka.clients.producer.Producer;


import eu.europa.ec.contentlayer.pod.producer.ProducerCreator;

public class App {
	private AppConfig appConfig;

	public static void main(String[] args) {
		ConsumerThread consumerThread = new ConsumerThread("ct1");
		consumerThread.t.start();
		runProducer();
		System.out.println("Data send");
		try {
			consumerThread.t.join();
		}
		catch (InterruptedException e) {
			System.out.println("Main thread interrupted");
		}

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
