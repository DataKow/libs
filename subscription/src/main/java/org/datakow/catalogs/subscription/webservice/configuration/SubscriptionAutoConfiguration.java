package org.datakow.catalogs.subscription.webservice.configuration;

import org.datakow.catalogs.subscription.webservice.SubscriptionWebserviceClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class enabled by the {@link EnableSubscriptionWebServiceClient} 
 * annotation to create the DAO bean used to interact with the Subscription Web Service.
 * 
 * @author kevin.off
 */
@Configuration
@EnableConfigurationProperties(SubscriptionConfigurationProperties.class)
public class SubscriptionAutoConfiguration {
    
    @Autowired
    SubscriptionConfigurationProperties props;
    
    /**
     * Creates the web service client bean.
     * 
     * @return The bean
     */
    @Bean
    public SubscriptionWebserviceClient subscriptionWebserviceClient(){
        return new SubscriptionWebserviceClient(
            "http://" + props.getWebserviceHost() + ":" + props.getWebservicePort()
        );
    } 
}
