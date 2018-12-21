package eu.europa.ec.contentlayer.pod.pojo;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.modify.request.UpdateWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ConsumerService {

    /**
     * Transform a model into an SPARQL insert query.
     *
     * @param model
     *  The subject model.
     * @param graphName
     *   The name of the graph where to insert the triples.
     * @return
     *   The formatted insert query.
     */
    public static String subjectModelToInsertQuery(Model model, String graphName) {
        // @todo Can we avoid all the stream wrapper madness?
        OutputStream outStream = new ByteArrayOutputStream();
        IndentedWriter writeout = new IndentedWriter(outStream);
        UpdateWriter updateWriter = new UpdateWriter(writeout, null);
        updateWriter.open();

        StmtIterator Triples = model.listStatements();
        while (Triples.hasNext()) {
            Statement Triple = Triples.nextStatement();
            Node graph = NodeFactory.createURI(graphName);
            updateWriter.insert(graph, Triple.asTriple());
        }

        updateWriter.close();
        writeout.flush();
        writeout.close();
        return (outStream).toString();
    }

    public static Model transactionToSubjectModel(ConsumerRecord<String, RdfTransaction> record) {
        Model model = ModelFactory.createDefaultModel();
        String serializedStatements = record.value().getStatements();

        InputStream stream = new ByteArrayInputStream(serializedStatements.getBytes(StandardCharsets.UTF_8));

        // read the RDF/XML file
        model.read(stream, null);
        return model;
    }
}
