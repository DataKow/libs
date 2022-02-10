package org.datakow.configuration.mongo;

import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

/**
 * Configuration class used to setup a connection to mongo. 
 * <p>
 * Creates the {@link MongoDatabaseFactory} bean
 * 
 * @author kevin.off
 */
@Configuration
@EnableConfigurationProperties(MongoConfigurationProperties.class)
@AutoConfigureBefore({MongoDataAutoConfiguration.class, MongoAutoConfiguration.class})
public class MongoCoreAutoConfiguration {

    @Autowired
    MongoConfigurationProperties props;

    @Value("${spring.application.name}")
    String applicationName;
    
    
    @Bean
    public MongoDatabaseFactory mongoFactory(MongoClient client) {
        
        return new SimpleMongoClientDatabaseFactory(client, props.getDatabaseName());
    }

    @Bean
    public MongoClient mongoClient(MongoClientSettings clientSettings) {
               
        return MongoClients.create(clientSettings);
    }

    @Bean
    public MongoClientSettings mongoClientSettings(ClusterSettings clusterSettings) {

        MongoClientSettings.Builder clientSettingsBuilder = MongoClientSettings.builder()
            .retryWrites(true)
            .writeConcern(props.getMongoWriteConcern())
            .readPreference(props.getMongoReadPreference())
            .applyToConnectionPoolSettings(builder -> {
                builder.maxSize(props.getConnPerHost())
                        .minSize(2)
                        .maxWaitTime(5000, TimeUnit.MILLISECONDS);
            })
            .applyToSocketSettings(builder -> {
                builder.connectTimeout(2000, TimeUnit.MILLISECONDS);
            })
            .applyToClusterSettings(builder -> builder.applySettings(clusterSettings))
            .applicationName(applicationName);

        if (props.getUseAuth()) {

            List<MongoCredential> credentials = new ArrayList<>();
            MongoCredential credential = MongoCredential.createCredential(
                props.getUserName(), 
                props.getDatabaseName(), 
                props.getPassword().toCharArray());
            credentials.add(credential);

            clientSettingsBuilder.credential(credential);
        }

        return clientSettingsBuilder.build();
    }

    @Bean 
    public ClusterSettings clusterSettings() {

        List<ServerAddress> addresses = new ArrayList<>();
        try{
            String[] serverNames = props.getServers().split(",");
            for(String serverName : serverNames){
                addresses.add(new ServerAddress(serverName.trim(), props.getPort()));
            }
        }catch(Exception e){
            Logger.getLogger(MongoCoreAutoConfiguration.class.getName()).log(Level.SEVERE, "There was an error creating the server address for mongodb", e);
        }

        return ClusterSettings.builder()
            .hosts(addresses)
            .build();
    }
    
    
}
