package org.datakow.messaging.events.configuration;

import org.datakow.configuration.rabbit.configuration.RabbitConfigurationProperties;
import org.datakow.messaging.events.EventsRoutingKeyBuilder;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class used in sending and receiving events.
 * 
 * @author kevin.off
 */
@Configuration
public class EventsConfiguration {
    
    @Value("${spring.profiles.active}")
    List<String> profiles;
    
    @Value("${spring.application.name}")
    String applicationName;
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;
   
    /**
     * Bean used to build the routing key to use on the DATAKOW events messaging system
     * @return the events routing key builder
     */
    @Bean
    public EventsRoutingKeyBuilder eventsRoutingKeyBuilder(){
        return new EventsRoutingKeyBuilder(rabbitProps.getMessagingVersion(), profiles.get(0), applicationName);
    }
    
}
