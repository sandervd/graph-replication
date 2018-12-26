package eu.europa.ec.contentlayer.pod.pojo;

import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import eu.europa.ec.contentlayer.pod.consumer.ConsumerCreator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;

public class ConsumerThread implements Runnable {
    String threadName;
    public Thread t;

    public ConsumerThread(String threadName) {
        this.threadName = threadName;
        this.t = new Thread(this, threadName);
    }

    static void runConsumer() {
        Consumer<String, RdfTransaction> consumer = ConsumerCreator.createConsumer();

        // URL of the remote RDF4J Server we want to access
        String serverUrl = "http://localhost:8080/rdf4j-server";
        RemoteRepositoryManager manager = new RemoteRepositoryManager(serverUrl);
        manager.initialize();
        Repository repo = manager.getRepository("pod-store");
        ValueFactory f = repo.getValueFactory();

        int noMessageToFetch = 0;
        try {
            RepositoryConnection con = repo.getConnection();
            con.begin();
            while (true) {
                final ConsumerRecords<String, RdfTransaction> consumerRecords = consumer.poll(200);
                if (consumerRecords.count() == 0) {
                    noMessageToFetch++;
                    System.out.println("Queue empty, attempt " + noMessageToFetch);
                    if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                        break;
                }
                consumerRecords.forEach(record -> {
                    LinkedHashModel model = ConsumerService.transactionToSubjectModel(record);
                    String graphName = "http://" + record.topic() + "/";
                    IRI context = f.createIRI(graphName);

                    System.out.println("Transaction: " + record.offset());
                    System.out.println(model.subjects().toString());
                    con.add(model, context);
                });
            }
            con.commit();
            consumer.commitAsync();
            con.close();
        } catch (RDF4JException e) {
            // handle exception
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
