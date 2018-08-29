package eu.europa.ec.contentlayer.pod;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;

import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdfconnection.RDFConnectionFuseki;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.apache.jena.sparql.modify.request.UpdateWriter;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
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

	/**
	 * Split a model in a list of models per subject.
	 *
	 * @param model The large model, containing multiple entities.
	 * @return Sub-models per entity.
	 */
	private static Map<String, Model> extractSubjectModels(Model model) {
		Map<String, Model> ExtractedModels = new HashMap<>();
		ResIterator AllResources = model.listSubjects();
		while (AllResources.hasNext()) {
			Resource ResourceToProcess = AllResources.nextResource();
			if (ResourceToProcess.isAnon()) {
				continue;
			}
			List<Statement> ResourceProperties = ResourceToProcess.listProperties().toList();
			Queue<Statement> StatementQueue = new LinkedList<>(ResourceProperties);
			Model ItemModel = ModelFactory.createDefaultModel();
			// 'recursively' fetch blank node statements.
			while (StatementQueue.size() > 0) {
				Statement StatementToProcess = StatementQueue.remove();
				if (StatementToProcess.getObject().isAnon()) {
					Resource subResource = StatementToProcess.getObject().asResource();
					List<Statement> SubResourceProperties = subResource.listProperties().toList();
					StatementQueue.addAll(SubResourceProperties);
				}
				ItemModel.add(StatementToProcess);
			}
			ExtractedModels.put(ResourceToProcess.getURI(), ItemModel);
		}
		return ExtractedModels;
	}

	static void runConsumer() {
		Consumer<String, RdfTransaction> consumer = ConsumerCreator.createConsumer();

        RDFConnectionRemoteBuilder builder = RDFConnectionFuseki.create()
                .destination("http://localhost:3030/datapod/query")
                .updateEndpoint("http://localhost:3030/datapod/update");

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
				Model model = ModelFactory.createDefaultModel();
                String serializedStatements = record.value().getStatements();

                InputStream stream = new ByteArrayInputStream(serializedStatements.getBytes(StandardCharsets.UTF_8));

				// read the RDF/XML file
				model.read(stream, null);

                OutputStream outStream = new ByteArrayOutputStream();
                IndentedWriter writeout = new IndentedWriter(outStream);
                UpdateWriter updateWriter = new UpdateWriter(writeout, null);
                updateWriter.open();

				StmtIterator Triples = model.listStatements();
				while (Triples.hasNext()) {
				    Statement Triple = Triples.nextStatement();
				    String URI = "http://" + record.topic() + "/";
                    Node graph = NodeFactory.createURI(URI);
                    updateWriter.insert(graph, Triple.asTriple());
                }

				updateWriter.close();
				writeout.flush();
                writeout.close();
                String insert = (outStream).toString();
                UpdateRequest query = UpdateFactory.create(insert);
                builder.build().update(query);
			});
			consumer.commitAsync();

		}
		consumer.close();
	}

	static void runProducer() {
		Producer<String, RdfTransaction> producer = ProducerCreator.createProducer();

		Model model = getModel();

		Map<String, Model> subjectModels = extractSubjectModels(model);
		subjectModels.forEach((subject, subjectModel) -> {
			System.out.println(serializeModel(subjectModel));
			RdfTransaction Transaction = buildRdfTransaction(subjectModel);
			commitTransaction(producer, Transaction, subject);
		});
	}

	private static RdfTransaction buildRdfTransaction(Model model) {
		RdfTransaction Transaction = new RdfTransaction();
		Transaction.setStatements(serializeModel(model));
		return Transaction;
	}

	private static Model getModel() {
		String inputFileName = "/home/sander/countries-skos.rdf";
		// String inputFileName = "/home/sander/eurovoc.rdf";

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

	private static String serializeModel(Model model) {
		OutputStream out = new ByteArrayOutputStream();
		model.write(out);
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
