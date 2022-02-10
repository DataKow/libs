package org.datakow.messaging.events.configuration;

import org.datakow.configuration.rabbit.configuration.RabbitConfigurationProperties;
import org.datakow.messaging.events.CatalogEventsSenderClient;
import org.datakow.messaging.events.events.Event;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.amqp.dsl.Amqp;

import org.springframework.messaging.handler.annotation.Header;

/**
 * Configuration used to setup a client to send events to the DATAKOW
 * events messaging system.
 * 
 * @author kevin.off
 */
@Configuration
@EnableIntegration
@IntegrationComponentScan
@Import(EventsConfiguration.class)
public class EventsSenderConfiguration {
    
    @Autowired
    ConnectionFactory connectionFactory;  
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;

    
    /**
     * Bean the represents the input channel where the messages flow to first to be routed to Rabbit.
     * 
     * @return The input channel reference
     */
    @Bean(name = "sendEvent.input")
    public PublishSubscribeChannel eventSenderInput(){
        return MessageChannels.publishSubscribe("sendEvent.input", Executors.newWorkStealingPool(8)).get();
    }
    
    /**
     * A {@link MessagingGateway} interface that connects client code to the Spring Integration flow to Rabbit.
     */
    @MessagingGateway(name = "CatalogEventsSenderGateway")
    public interface CatalogEventsSenderGateway {
        
        @Gateway(requestChannel = "sendEvent.input")
        public void sendEvent(Event event, @Header("routingKey") String routingKey, @Header("Request-ID") String requestId, @Header("Correlation-ID") String correlationId);
        
    }
    
    /**
     * The actual integration flow object that receives messages on the input channel
     * and sends them to RabbitMQ.
     * 
     * @return The Spring integration flow
     */
    @Bean
    public IntegrationFlow sendCatalogEvent(){
        return IntegrationFlows.from("sendEvent.input")
                .wireTap(m->m.handle((mm)->Logger.getLogger(EventsSenderConfiguration.class.getName()).log(Level.FINE, "About to send an event: {0}", ((Event)mm.getPayload()).getEventId())))
                .handle(Amqp.outboundAdapter(catalogEventsTemplate())
                        .exchangeName(rabbitProps.getAppExchangeName())
                        .mappedRequestHeaders("*")
                        .routingKeyExpression("headers.routingKey"))
                .channel("nullChannel").get();
       
    }
    
    /**
     * The message converter used to convert the events to JSON messages that are sent to Rabbit.
     * 
     * @return The converter
     */
    public Jackson2JsonMessageConverter catalogEventsMessageConverter(){
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        DefaultClassMapper mapper = new DefaultClassMapper();
        mapper.setDefaultType(Event.class);
        converter.setClassMapper(mapper);
        return converter;
    } 
    
    /**
     * The underlying RabbitTemplate DAO object used to perform actions on Rabbit.
     * 
     * @return The RabbitTemplate bean
     */
    @Bean
    public RabbitTemplate catalogEventsTemplate(){
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(catalogEventsMessageConverter());
        return template;
    }
    
    /**
     * The client bean used to send messages.
     * <p>
     * The bean may be used by Autowiring it in any of your component classes
     * 
     * @return The client bean
     */
    @Bean
    public CatalogEventsSenderClient eventsSenderClient(){
        return new CatalogEventsSenderClient();
    }

    @Bean
    public TopicExchange declareEventServiceExchange(RabbitConfigurationProperties props) {
        TopicExchange exchange = new TopicExchange(props.getAppExchangeName(), true, true);
        return exchange;
    }

    @Bean
    public Binding declareEventServiceExchangeBinding(RabbitConfigurationProperties props) {
        Binding binding = new Binding("e.events", DestinationType.EXCHANGE, props.getEventsExchangeName(), "#", null);
        return binding;
    }
}
