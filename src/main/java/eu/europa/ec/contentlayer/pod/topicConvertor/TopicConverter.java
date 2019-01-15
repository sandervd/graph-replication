package eu.europa.ec.contentlayer.pod.topicConvertor;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

// @todo Replace this naive implementation with something a bit more robust.

public class TopicConverter {
    public static String toTopic(String graphName) {
        return graphName
                .replace("http://", "")
                .replace("/", ".");
    }

}
