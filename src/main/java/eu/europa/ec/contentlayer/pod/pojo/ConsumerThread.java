package eu.europa.ec.contentlayer.pod.pojo;

import eu.europa.ec.contentlayer.pod.constants.IKafkaConstants;
import eu.europa.ec.contentlayer.pod.consumer.ConsumerService;
import eu.europa.ec.contentlayer.pod.transaction.materialization.DataMaterializationEvent;
import eu.europa.ec.contentlayer.pod.transaction.materialization.DataMaterializationService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class ConsumerThread extends Thread {
    private DataMaterializationService dataMaterializationService;
    private ConsumerService consumerService;

    @Autowired
    public ConsumerThread(DataMaterializationService dataMaterializationService, ConsumerService consumerService) {
        this.consumerService = consumerService;
        this.dataMaterializationService = dataMaterializationService;
    }

    void runConsumer() {
        Consumer<String, RdfTransaction> consumer = this.consumerService.getInstance();


        // URL of the remote RDF4J Server we want to access
        String serverUrl = "http://localhost:8080/rdf4j-server";
        RemoteRepositoryManager manager = new RemoteRepositoryManager(serverUrl);
        manager.initialize();
        Repository storageRepository = manager.getRepository("pod-store");

        int noMessageToFetch = 0;
        try {
            RepositoryConnection storageRepositoryConnection = storageRepository.getConnection();
            storageRepositoryConnection.begin();
            while (true) {
                final ConsumerRecords<String, RdfTransaction> consumerRecords = consumer.poll(200);
                if (consumerRecords.count() == 0) {
                    noMessageToFetch++;
                    System.out.println("Queue empty, attempt " + noMessageToFetch);
                    if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                        break;
                }
                consumerRecords.forEach(record -> {
                    Model model = eu.europa.ec.contentlayer.pod.pojo.ConsumerService.transactionToSubjectModel(record);
                    DataMaterializationEvent transactionEvent = this.dataMaterializationService.prepareTransaction(storageRepository, record, model);
                    storageRepositoryConnection.add(transactionEvent.getModel(), transactionEvent.getTargetGraph());
                });
            }
            storageRepositoryConnection.commit();
            consumer.commitAsync();
            storageRepositoryConnection.close();
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
