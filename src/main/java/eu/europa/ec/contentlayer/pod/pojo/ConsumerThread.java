package eu.europa.ec.contentlayer.pod.pojo;

import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import eu.europa.ec.contentlayer.pod.consumer.ConsumerCreator;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdfconnection.RDFConnectionFuseki;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerThread implements Runnable {
    String threadName;
    public Thread t;

    public ConsumerThread(String threadName) {
        this.threadName = threadName;
        this.t = new Thread(this, threadName);
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
                System.out.println("Queue empty, attempt " + noMessageToFetch);
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

    @Override
    public void run() {
        System.out.println("Start consuming...");
        runConsumer();
        System.out.println("Data consumed");

    }
}
