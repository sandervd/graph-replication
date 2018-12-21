package eu.europa.ec.contentlayer.pod.pojo;

import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.FileManager;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ProducerService {
    /**
     * Split a model in a list of models per subject (subject models).
     *
     * @param model The large model, containing multiple entities.
     * @return Sub-models per entity.
     */
    public static Map<String, Model> extractSubjectModels(Model model) {
        Map<String, Model> ExtractedModels = new HashMap<>();
        ResIterator AllResources = model.listSubjects();
        while (AllResources.hasNext()) {
            Resource ResourceToProcess = AllResources.nextResource();
            if (ResourceToProcess.isAnon()) {
                continue;
            }
            Model ItemModel = expandBlankNodes(ResourceToProcess);
            ExtractedModels.put(ResourceToProcess.getURI(), ItemModel);
        }
        return ExtractedModels;
    }

    /**
     * Expand a resource into a subject model; containing all properties related to the subject.
     *
     * @param ResourceToProcess
     * 	The resource object of the subject to expand.
     * @return
     *  The subject model.
     */
    private static Model expandBlankNodes(Resource ResourceToProcess) {
        List<Statement> ResourceProperties = ResourceToProcess.listProperties().toList();
        Queue<Statement> StatementQueue = new LinkedList<>(ResourceProperties);
        Model ItemModel = ModelFactory.createDefaultModel();
        while (StatementQueue.size() > 0) {
            Statement StatementToProcess = StatementQueue.remove();
            if (StatementToProcess.getObject().isAnon()) {
                Resource subResource = StatementToProcess.getObject().asResource();
                List<Statement> SubResourceProperties = subResource.listProperties().toList();
                StatementQueue.addAll(SubResourceProperties);
            }
            ItemModel.add(StatementToProcess);
        }
        return ItemModel;
    }

    public static RdfTransaction buildRdfTransaction(Model model) {
        RdfTransaction Transaction = new RdfTransaction();
        Transaction.setStatements(serializeModel(model));
        return Transaction;
    }

    public static Model buildModelFromFile(String FileName) {
        // create an empty model
        Model model = ModelFactory.createDefaultModel();

        // use the FileManager to find the input file
        InputStream in = FileManager.get().open( FileName );
        if (in == null) {
            throw new IllegalArgumentException(
                    "File: " + FileName + " not found");
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

    public static void commitTransaction(Producer<String, RdfTransaction> producer, RdfTransaction transaction, String Subject) {
        final ProducerRecord<String, RdfTransaction> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, null, Subject, transaction);
        try {
            RecordMetadata metadata = producer.send(record).get();
            // System.out.println("Record sent to partition " + metadata.partition()
            //		+ " with offset " + metadata.offset());
        }
        // @todo Rethrow exception to cancel triplestore transaction.
        catch (ExecutionException |InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }
}
