package eu.europa.ec.contentlayer.pod.datasource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DataSourceDiscoveryFactory {
    @Value("${datasource.sor}")
    private String dataSourceSOR;

    private LocalDataSourceDiscovery local;
    private KafkaDataSourceDiscovery kafka;

    @Autowired
    public DataSourceDiscoveryFactory(LocalDataSourceDiscovery local, KafkaDataSourceDiscovery kafka) {
        // @todo Fix this ridiculous mess of factory/IoC madness.
        // Inject container, or swap services?
        this.local = local;
        this.kafka = kafka;
    }

    public synchronized DataSourceDiscovery getInstance() {
        if (this.dataSourceSOR.equals("local")) {
            return this.local;
        }
        return this.kafka;
    }
}
