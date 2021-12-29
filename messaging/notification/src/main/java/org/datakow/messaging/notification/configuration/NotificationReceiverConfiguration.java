package org.datakow.messaging.notification.configuration;


import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.configuration.rabbit.configuration.RabbitConfigurationProperties;
import org.datakow.messaging.notification.NotificationReceiver;
import org.datakow.messaging.notification.NotificationReceiverClient;
import org.datakow.messaging.notification.notifications.Notification;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

/**
 * Configuration used to setup a client to receive notifications from the DATAKOW
 * notifications messaging system.
 * 
 * @author kevin.off
 */
@Configuration
@EnableIntegration
@Import(NotificationConfiguration.class)
@EnableConfigurationProperties(CatalogsNotificationReceiverConfigurationProperties.class)
public class NotificationReceiverConfiguration {
    
    @Autowired
    ConnectionFactory connectionFactory;
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;
    
    @Autowired
    CatalogsNotificationReceiverConfigurationProperties props;

    @Bean
    public SimpleMessageListenerContainer simpleMessageListenerContainer(
        SimpleRabbitListenerContainerFactory factory) {

        return factory.createListenerContainer();

    }

    @Bean(name = "rabbitListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory() {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(notificationMessageConverter());
        factory.setAutoStartup(false);
        factory.setPrefetchCount(rabbitProps.getNotificationsPrefetchCount());
        if (props.getConcurrentConsumers() > 1){
            factory.setConcurrentConsumers(props.getConcurrentConsumers());
        }
        return factory;
    }
    
    /**
     * Creates the flow of messages from RabbitMQ to the client receiving the notification.
     * 
     * @param receiver The client implemented receiver interface to send the messages to
     * @return The flow object to be registered with spring
     */
    @Bean
    @ConditionalOnBean(NotificationReceiver.class)
    public IntegrationFlow notificationReceiverFlow(
        NotificationReceiver receiver, 
        SimpleMessageListenerContainer container){
        
        return IntegrationFlows.from(Amqp.inboundAdapter(container)
                .autoStartup(false)
                .mappedRequestHeaders("*")
                .messageConverter(notificationMessageConverter())
        ).handle((message) -> {
            String queueName = message.getHeaders().get(AmqpHeaders.CONSUMER_QUEUE).toString();
            String subscriptionId = queueName.replace("q.subscriber.", "");
            
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
                ThreadContext.put("catalogIdentifier", ((Notification)message.getPayload()).getObjectMetadataIdentity().getCatalogIdentifier());
                ThreadContext.put("recordIdentifier", ((Notification)message.getPayload()).getObjectMetadataIdentity().getRecordIdentifier());
            }catch(Exception ex){
                Logger.getLogger(NotificationReceiverConfiguration.class.getName()).log(Level.SEVERE, "Error setting ThreadContext when receiving notification", ex);
            }
            try{
                Logger.getLogger(NotificationReceiverConfiguration.class.getName()).log(Level.INFO, "Received notification {0}", ((Notification)message.getPayload()).toJson());
            }catch(JsonProcessingException ex){
                Logger.getLogger(NotificationReceiverConfiguration.class.getName()).log(Level.SEVERE, "Error while parsing notification to JSON when receiving", ex);
            }
            try{
                receiver.receiveNotification((Notification)message.getPayload(), subscriptionId, queueName);
            }finally{
                ThreadContext.clearAll();
            }
        }).get();
    }
    
    /**
     * The converter used to interpret the messages coming from rabbit and to convert them to Event objects. 
     * @return The converter
     */
    public Jackson2JsonMessageConverter notificationMessageConverter(){
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        
        //TODO: This is a temporary measure. When the catalogs get replaced with 2.0.0 then this can go away
        Map<String, Class<?>> mappings = new HashMap<>();
        mappings.put("org.datakow.catalogs.notification.Notification", Notification.class);
        
        DefaultClassMapper mapper = new DefaultClassMapper();
        mapper.setDefaultType(Notification.class);
        mapper.setIdClassMapping(mappings);
        converter.setClassMapper(mapper);
        converter.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
        return converter;
    }
    
    /**
     * The client bean used to configure what notifications are received and commands to start and stop the listening.
     * 
     * @return The client bean
     */
    @Bean
    public NotificationReceiverClient notificationReceiverClient(
        SimpleMessageListenerContainer container){

        return new NotificationReceiverClient(container, rabbitProps.getServicesClientsExchangeName());
    }
    
}
