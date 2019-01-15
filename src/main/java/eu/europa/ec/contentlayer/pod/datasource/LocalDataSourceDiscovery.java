package eu.europa.ec.contentlayer.pod.datasource;

import eu.europa.ec.contentlayer.pod.consumer.ConsumerService;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

@Component
public class LocalDataSourceDiscovery implements DataSourceDiscovery {

    @Value("${pod.catalogue}")
    private String podIdentity;

    private Model model;

    private ConsumerService consumerService;


    @Autowired
    public LocalDataSourceDiscovery(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @PostConstruct
    public void rebuildState() {
         ValueFactory vf = SimpleValueFactory.getInstance();
        IRI podIdentity = vf.createIRI(this.podIdentity);
        Set<String> topics = new HashSet<>();
        try {
            InputStream inputStream = new FileInputStream("catalogue.ttl");
            this.model = Rio.parse(inputStream, "", RDFFormat.TURTLE);
            inputStream.close();
            DataSourceRepository repo = new DataSourceRepository(this.model, podIdentity);

            topics.add("demo");
            this.consumerService.getInstance().subscribe(topics);
        }

        catch (Exception e) {
            System.out.println("Could not read local catalogue.");
            throw new RuntimeException(e);
        }
    }
}
