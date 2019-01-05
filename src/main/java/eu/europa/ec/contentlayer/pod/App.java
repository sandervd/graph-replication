package eu.europa.ec.contentlayer.pod;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import eu.europa.ec.contentlayer.pod.datasource.DataSourceDiscovery;
import eu.europa.ec.contentlayer.pod.datasource.DataSourceDiscoveryFactory;
import eu.europa.ec.contentlayer.pod.pojo.*;
import org.apache.kafka.clients.producer.Producer;


import eu.europa.ec.contentlayer.pod.producer.ProducerCreator;
import org.eclipse.rdf4j.rio.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class App {
	private AppConfig appConfig;

	public static void main(String[] args) {
		ConfigurableApplicationContext cont = SpringApplication.run(App.class, args);

		DataSourceDiscoveryFactory dataSourceDiscoveryFactory = (DataSourceDiscoveryFactory) cont.getBean("dataSourceDiscoveryFactory");
		DataSourceDiscovery ds = dataSourceDiscoveryFactory.getInstance();
		ds.rebuildState();
		System.out.println(ds.getClass());

		//AppConfig appConfig = new AppConfig();
		for(String serviceName : cont.getBeanDefinitionNames()) {
			System.out.println(serviceName);
		}

		ConsumerThread consumerThread = (ConsumerThread) cont.getBean("consumerThread");
		consumerThread.setName("Consumer thread 1");
		consumerThread.start();

		runProducer();
		System.out.println("Data send");
		try {
			consumerThread.join();
		}
		catch (InterruptedException e) {
			System.out.println("Main thread interrupted");
		}

	}

	static void runProducer() {
		Producer<String, RdfTransaction> producer = ProducerCreator.createProducer();

		try {
			InputStream inputStream = new FileInputStream("/home/sander/eurovoc.rdf");
			//InputStream inputStream = new FileInputStream("/home/sander/countries-skos.rdf");
			RDFParser rdfParser = Rio.createParser(RDFFormat.RDFXML);
			ModelBySubject subjectModelList = new ModelBySubject();
			rdfParser.setRDFHandler(subjectModelList);
			try {
				rdfParser.parse(inputStream, "http://example.org/");
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
				inputStream.close();
			}
			//Map<String, LinkedHashModel> subjectModels = ProducerService.extractSubjectModels(model);
			System.out.println("Model parsed");

			subjectModelList.getModels().forEach((subject, subjectModel) -> {
				RdfTransaction Transaction = ProducerService.buildRdfTransaction(subjectModel);
				ProducerService.commitTransaction(producer, Transaction, subject);
			});
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
}
