package org.datakow.messaging.events.configuration;


import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.configuration.rabbit.configuration.RabbitConfigurationProperties;
import org.datakow.messaging.events.CatalogEventsReceiver;
import org.datakow.messaging.events.CatalogEventsReceiverClient;
import org.datakow.messaging.events.events.CatalogEvent;
import org.datakow.messaging.events.events.DataFileEvent;
import org.datakow.messaging.events.events.Event;
import org.datakow.messaging.events.events.RecordAssociationEvent;
import org.datakow.messaging.events.events.RecordEvent;
import org.datakow.messaging.events.events.SubscriptionEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.amqp.dsl.Amqp;

/**
 * Configuration used to setup a client to receive events from the DATAKOW
 * events messaging system.
 * 
 * @author kevin.off
 */
@Configuration
@EnableIntegration
@Import(EventsConfiguration.class)
@EnableConfigurationProperties(EventReceiverConfigurationProperties.class)
public class EventsReceiverConfiguration {
 
    @Autowired
    ConnectionFactory connectionFactory;  
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;
    
    @Autowired
    EventReceiverConfigurationProperties props;
    
    @Value("${spring.application.name}")
    private String applicatioName;
    
    /**
     * Creates the flow of messages from RabbitMQ to the client receiving the event.
     * 
     * @param receiver The client implemented receiver interface to send the messages to
     * @return The flow object to be registered with spring
     */
    @Bean
    public IntegrationFlow receiveEventsBean(
        CatalogEventsReceiver receiver, 
        SimpleMessageListenerContainer container){

        return IntegrationFlows.from(
                Amqp.inboundAdapter(container)
                        .autoStartup(false)
                        .mappedRequestHeaders("*")
                        .messageConverter(catalogEventsMessageConverter()))
                .handle((message)->{
                        String correlationId = message.getHeaders().get("Correlation-ID", String.class);
                        String requestId = message.getHeaders().get("Request-ID", String.class);
                        if (correlationId == null || correlationId.isEmpty()){
                            correlationId = UUID.randomUUID().toString();
                        }
                        if (requestId == null || requestId.isEmpty()){
                            requestId = UUID.randomUUID().toString();
                        }
                        ThreadContext.put("requestId", requestId);
                        ThreadContext.put("correlationId", correlationId);
                        
                        try{
                            if (message.getPayload() instanceof RecordEvent){
                                ThreadContext.put("catalogIdentifier", ((RecordEvent)message.getPayload()).getCatalogIdentity().getCatalogIdentifier());
                                ThreadContext.put("recordIdentifier", ((RecordEvent)message.getPayload()).getCatalogIdentity().getRecordIdentifier());
                            }else if (message.getPayload() instanceof RecordAssociationEvent){
                                ThreadContext.put("catalogIdentifier", ((RecordAssociationEvent)message.getPayload()).getObjectMetadataIdentity().getCatalogIdentifier());
                                ThreadContext.put("recordIdentifier", ((RecordAssociationEvent)message.getPayload()).getObjectMetadataIdentity().getRecordIdentifier());
                            }else if (message.getPayload() instanceof CatalogEvent){
                                ThreadContext.put("catalogIdentifier", ((CatalogEvent)message.getPayload()).getCatalogIdentifier());
                                ThreadContext.remove("recordIdentifier");
                            }else if (message.getPayload() instanceof SubscriptionEvent){
                                ThreadContext.put("catalogIdentifier", "subscriptions");
                                ThreadContext.put("recordIdentifier", ((SubscriptionEvent)message.getPayload()).getSubscriptionId());
                            }  
                        }catch(Exception ex){
                            Logger.getLogger(EventsReceiverConfiguration.class.getName()).log(Level.SEVERE, "Error setting ThreadContext when receiving event", ex);
                        }
                        try {
                            Logger.getLogger(EventsReceiverConfiguration.class.getName()).log(Level.INFO, "Received event {0}", ((Event)message.getPayload()).toJson());
                        } catch (JsonProcessingException ex) {
                            Logger.getLogger(EventsReceiverConfiguration.class.getName()).log(Level.SEVERE, "There was an error converting the event to JSON when logging for receive", ex);
                        }
                        try{
                            receiver.receiveEvent((Event)message.getPayload());
                        }finally{
                            ThreadContext.clearAll();
                        }
                    })
                .get();
    }

    @Bean
    public SimpleMessageListenerContainer simpleMessageListenerContainer(
        SimpleRabbitListenerContainerFactory factory) {

        return factory.createListenerContainer();

    }

    @Bean(name = "rabbitListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory() {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(catalogEventsMessageConverter());
        factory.setAutoStartup(false);
        factory.setPrefetchCount(rabbitProps.getEventsPrefetchCount());
        if (props.getConcurrentConsumers() > 1){
            factory.setConcurrentConsumers(props.getConcurrentConsumers());
        }
        return factory;
    }
    
    /**
     * The converter used to interpret the messages coming from rabbit and to convert them to Event objects. 
     * @return The converter
     */
    public Jackson2JsonMessageConverter catalogEventsMessageConverter() {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        
        //TODO: This is a temporary measure. When the catalogs get replaced with 2.0.0 then this can go away
        Map<String, Class<?>> mappings = new HashMap<>();
        mappings.put("org.datakow.catalogs.events.CatalogEvent", CatalogEvent.class);
        mappings.put("org.datakow.catalogs.events.DataFileEvent", DataFileEvent.class);
        mappings.put("org.datakow.catalogs.events.RecordAssociationEvent", RecordAssociationEvent.class);
        mappings.put("org.datakow.catalogs.events.RecordEvent", RecordEvent.class);
        mappings.put("org.datakow.catalogs.events.SubscriptionEvent", SubscriptionEvent.class);
        mappings.put("org.datakow.catalogs.events.Event", Event.class);
        
        
        DefaultClassMapper mapper = new DefaultClassMapper();
        mapper.setDefaultType(Event.class);
        mapper.setIdClassMapping(mappings);
        converter.setClassMapper(mapper);
        converter.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
        return converter;
    } 
    
    /**
     * The client bean used to configure what events are received and commands to start and stop the listening.
     * 
     * @return The client bean
     */
    @Bean
    public CatalogEventsReceiverClient eventsReceiverClient(
        SimpleMessageListenerContainer container){
        return new CatalogEventsReceiverClient(
                container, 
                rabbitProps.getEventsExchangeName(), 
                applicatioName, 
                rabbitProps.getMessagingVersion());
    }
    
}
