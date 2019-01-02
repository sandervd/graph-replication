package eu.europa.ec.contentlayer.pod.transaction.materialization;

import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.repository.Repository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Component;

@Component
public class DataMaterializationService implements ApplicationEventPublisherAware {
    private ApplicationEventPublisher publisher;

    public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    public DataMaterializationEvent prepareTransaction(Repository repository, ConsumerRecord<String, RdfTransaction> record, Model model) {
        DataMaterializationEvent event = new DataMaterializationEvent(this, repository, record, model);
        publisher.publishEvent(event);
        return event;
    }
}
