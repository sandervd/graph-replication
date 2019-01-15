package eu.europa.ec.contentlayer.pod.datasource;

import eu.europa.ec.contentlayer.pod.vocabulary.DCATAP;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.util.HashSet;
import java.util.Set;

public class DataSourceRepository {
    private Model model;

    private IRI podIdentity;

    private Set<DataSet> dataSourceList = new HashSet<>();

    public DataSourceRepository(Model model, IRI podIdentity) {
        this.model = model;
        this.podIdentity = podIdentity;
        modelToDataSet();
    }

    private void modelToDataSet() {
        Model datasets = model.filter(null, RDF.TYPE, DCATAP.DATASET);
        for(Statement dataset : datasets) {
            boolean authoritative = false;
            Model AuthSets = model.filter(dataset.getSubject(), DCATAP.AUTHCATALOGUE, podIdentity);
            if (!AuthSets.subjects().isEmpty()) {
                authoritative = true;
            }
            dataSourceList.add(new DataSet(dataset.getSubject().toString(), dataset.getSubject().toString(), authoritative));
        }
    }
}
