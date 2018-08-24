package eu.europa.ec.contentlayer.pod.deserializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europa.ec.contentlayer.pod.pojo.RdfTransaction;

public class RdfTransactionDeserializer implements Deserializer<RdfTransaction> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public RdfTransaction deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		RdfTransaction object = null;
		try {
			object = mapper.readValue(data, RdfTransaction.class);
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
		}
		return object;
	}

	@Override
	public void close() {
	}
}