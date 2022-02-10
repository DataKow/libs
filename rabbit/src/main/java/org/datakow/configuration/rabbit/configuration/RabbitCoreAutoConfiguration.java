package org.datakow.configuration.rabbit.configuration;


import org.datakow.configuration.rabbit.RabbitClient;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Configuration class enabled by the {@link EnableRabbit} annotation to create
 * beans necessary to connect to Rabbit.
 * <p>
 * This class requires that you have the properties for the {@link RabbitConfigurationProperties} bean and
 * the spring.application.name properties set.
 * 
 * @author kevin.off
 */
@Configuration
@EnableConfigurationProperties(RabbitConfigurationProperties.class)
public class RabbitCoreAutoConfiguration {
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;
    
    @Value("${spring.application.name}")
    String applicationName;
    
    
    /**
     * Creates the RabbitMQ caching connection factory for use to connect to Rabbit
     * based on the properties in the {@link RabbitConfigurationProperties}
     * @param config The configuration
     * @return The factory bean
     */
    @Bean
    public ConnectionFactory rabbitConnectionFactory(RabbitConfigurationProperties config){
        CachingConnectionFactory factory = new CachingConnectionFactory();

        if (config.getHostName()!= null) {
                factory.setHost(config.getHostName());
                factory.setPort(config.getPort());
        }
        if (config.getAppId()!= null) {
                factory.setUsername(config.getAppId());
        }
        if (config.getPassword() != null) {
                factory.setPassword(config.getPassword());
        }
        if (config.getVirtualHost() != null) {
                factory.setVirtualHost(config.getVirtualHost());
        }
        if (config.getChannelCacheSize() != 0){
                factory.setChannelCacheSize(config.getChannelCacheSize());
        }
        if (config.getChannelCheckoutTimeout() != 0){
                factory.setChannelCheckoutTimeout(config.getChannelCheckoutTimeout());
        }
        //factory.setPublisherConfirms(true);
        
        return factory;
    }
    
    /**
     * Creates the Spring Rabbit Admin class that is used by the DATAKOW {@link RabbitClient}
     * 
     * @param connectionFactory The configured connection factory bean
     * @return The bean
     */
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory){
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        return admin;
    }

    
    /**
     * The DATAKOW Rabbit Admin used to interact with RabbitQM
     * 
     * @param rabbitAdmin The RabbitAdmin bean
     * @return The Rabbit Admin bean
     */
    @Bean
    RabbitClient rabbitClient(RabbitAdmin rabbitAdmin){
        return new RabbitClient(rabbitAdmin);
    }
    
    /**
     * The e.services.clients exchange that client queues are bound to and where
     * SUBSCRIBED and UNSUBSCRIBED notifications are sent.
     * 
     * @param props The configuration properties
     * @return the e.services.clients exchange object
     */
    @Bean(name = "e_services_clients")
    public DirectExchange eServicesClients(RabbitConfigurationProperties props){
        //                        Name               Durable, AutoDelete
        DirectExchange exchange = new DirectExchange(props.getServicesClientsExchangeName(), true, false);
        return exchange;
    }
    
    /**
     * A reference to the events exchange
     * 
     * @param props The configuration properties
     * @return The exchange
     */
    @Bean(name = "e_events")
    public TopicExchange eEvents(RabbitConfigurationProperties props) {
        //                                              Name                    Durable, AutoDelete
        TopicExchange exchange = new TopicExchange(props.getEventsExchangeName(), true, false);
        return exchange;
    }
    
}
