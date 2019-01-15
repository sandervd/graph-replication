package eu.europa.ec.contentlayer.pod.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.DCAT;

public class DCATAP extends DCAT {
    public static final String PREFIX = "dcat-ap";
    public static final String NAMESPACE = "http://data.europa.eu/temp/ontology/dcat-ap#";
    public static final Namespace NS = new SimpleNamespace("dcat", "http://data.europa.eu/temp/ontology/dcat-ap#");
    public static final IRI AUTHCATALOGUE;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        AUTHCATALOGUE = factory.createIRI(NAMESPACE, "hasAuthoritativeCatalogue");
    }
}
