package eu.europa.ec.contentlayer.pod.pojo;

import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;

public class ProducerService {


    public static RdfTransaction buildRdfTransaction(LinkedHashModel model) {
        RdfTransaction Transaction = new RdfTransaction();

        Transaction.setStatements(serializeModel(model));
        return Transaction;
    }

    private static String serializeModel(LinkedHashModel model) {
        OutputStream out = new ByteArrayOutputStream();
        RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, out);
        try {
            writer.startRDF();
            for (org.eclipse.rdf4j.model.Statement st: model) {
                writer.handleStatement(st);
            }
            writer.endRDF();
        }
        catch (RDFHandlerException e) {
            System.out.println(e.toString());
        }
        finally {
            try {
                out.close();
            }
            catch (Exception e) {
                System.out.println(e.toString());
            }
        }
        return out.toString();
    }

    public static void commitTransaction(Producer<String, RdfTransaction> producer, RdfTransaction transaction, String Subject) {
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
