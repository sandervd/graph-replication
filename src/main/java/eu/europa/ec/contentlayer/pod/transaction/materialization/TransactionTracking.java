package eu.europa.ec.contentlayer.pod.transaction.materialization;

import eu.europa.ec.contentlayer.pod.vocabulary.DTO;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.HashSet;

@Component
public class TransactionTracking implements ApplicationListener<DataMaterializationEvent> {
    public void onApplicationEvent(DataMaterializationEvent event) {
        long offset = event.getRecord().offset();
        String aggregateSubject = event.getRecord().key();
        IRI aggregateSubjectIRI = event.getRepository().getValueFactory().createIRI(aggregateSubject);
        Model model = new LinkedHashModel(event.getModel());
        Literal True = event.getRepository().getValueFactory().createLiteral(true);
        Literal TransactionID = event.getRepository().getValueFactory().createLiteral(offset);

        HashSet<Resource> allSubjects = new HashSet<>();
        allSubjects.addAll(model.subjects());
        for (Resource subject : allSubjects) {
            model.add(subject, DTO.TRANSACTIONID, TransactionID);
        }
        model.add(aggregateSubjectIRI, DTO.ISAGGREGATEROOT, True);
        event.setModel(model);
    }
}
