package eu.europa.ec.contentlayer.pod.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Database transaction ontology.
 */
public class DTO {
    public static final String PREFIX = "DTO";
    public static final String NAMESPACE = "http://data.europa.eu/ns/dto#";
    public static final Namespace NS = new SimpleNamespace("dto", "http://data.europa.eu/ns/dto#");
    public static final IRI ISAGGREGATEROOT;
    public static final IRI TRANSACTIONID;

    public DTO() {
    }

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        ISAGGREGATEROOT = factory.createIRI("http://data.europa.eu/ns/dto#", "isAggregateRoot");
        TRANSACTIONID = factory.createIRI("http://data.europa.eu/ns/dto#", "transactionId");
    }
}

