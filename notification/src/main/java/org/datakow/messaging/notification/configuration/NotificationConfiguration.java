package org.datakow.messaging.notification.configuration;

import org.datakow.configuration.rabbit.configuration.RabbitConfigurationProperties;
import org.datakow.messaging.notification.ServicesRoutingKeyBuilder;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class used in sending and receiving notifications.
 * 
 * @author kevin.off
 */
@Configuration
public class NotificationConfiguration {
    
    @Value("${spring.profiles.active}")
    List<String> profiles;
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;
    
    /**
     * Bean used to build the routing key to use on the DATAKOW notification messaging system
     * @return the notification routing key builder
     */
    @Bean
    public ServicesRoutingKeyBuilder servicesRoutingKeyBuilder(){
        return new ServicesRoutingKeyBuilder(rabbitProps.getMessagingVersion(), profiles.get(0));
    }
    
}
