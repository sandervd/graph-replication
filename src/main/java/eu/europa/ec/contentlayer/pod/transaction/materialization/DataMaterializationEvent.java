package eu.europa.ec.contentlayer.pod.transaction.materialization;

import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.repository.Repository;
import org.springframework.context.ApplicationEvent;

public class DataMaterializationEvent extends ApplicationEvent {
    private Model model;
    private ConsumerRecord<String, RdfTransaction> record;
    private Repository repository;
    private IRI targetGraph;

    public DataMaterializationEvent(Object source, Repository repository, ConsumerRecord<String, RdfTransaction> record, Model model) {
        super(source);
        this.repository = repository;
        this.record = record;
        this.model = model;
    }

    public Repository getRepository() {
        return repository;
    }

    public IRI getTargetGraph() {
        return targetGraph;
    }

    public void setTargetGraph(IRI targetGraph) {
        this.targetGraph = targetGraph;
    }

    public Model getModel() {
        return model;
    }

    public void setModel(Model model) {
        this.model = model;
    }

    public ConsumerRecord<String, RdfTransaction> getRecord() {
        return record;
    }

    public void setRecord(ConsumerRecord<String, RdfTransaction> record) {
        this.record = record;
    }
}
