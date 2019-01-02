package eu.europa.ec.contentlayer.pod.transaction.materialization;

import org.eclipse.rdf4j.model.IRI;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class TargetGraph implements ApplicationListener<DataMaterializationEvent> {
    public void onApplicationEvent(DataMaterializationEvent event) {
        String graphName = "http://" + event.getRecord().topic() + "/";
        IRI graph = event.getRepository().getValueFactory().createIRI(graphName);
        event.setTargetGraph(graph);
    }
}
