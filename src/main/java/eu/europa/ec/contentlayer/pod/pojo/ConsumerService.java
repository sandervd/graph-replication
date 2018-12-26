package eu.europa.ec.contentlayer.pod.pojo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ConsumerService {

    public static LinkedHashModel transactionToSubjectModel(ConsumerRecord<String, RdfTransaction> record) {
        LinkedHashModel model = new LinkedHashModel();
        RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
        String serializedStatements = record.value().getStatements();

        InputStream stream = new ByteArrayInputStream(serializedStatements.getBytes(StandardCharsets.UTF_8));
        rdfParser.setRDFHandler(new StatementCollector(model));

        // read the RDF/XML file
        try {
            rdfParser.parse(stream, "http://no-idea-why-i-need-a-host-here/");
        }
        catch (IOException e) {
            // handle IO problems (e.g. the file could not be read)
        }
        catch (RDFParseException e) {
            // handle unrecoverable parse error
        }
        catch (RDFHandlerException e) {
            // handle a problem encountered by the RDFHandler
        }
        finally {
            try {
                stream.close();
            }
            catch (Exception e) {
                System.out.println(e.toString());
            }
        }
        return model;
    }
}
