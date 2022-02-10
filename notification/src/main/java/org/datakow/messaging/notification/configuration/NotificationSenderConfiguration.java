package org.datakow.messaging.notification.configuration;

import org.datakow.configuration.rabbit.RabbitClient;
import org.datakow.configuration.rabbit.configuration.RabbitConfigurationProperties;
import org.datakow.messaging.notification.NotificationSenderClient;
import org.datakow.messaging.notification.notifications.Notification;
import java.util.List;
import java.util.concurrent.Executors;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.handler.annotation.Header;

/**
 * Configuration used to setup a client to send notifications to the DATAKOW
 * notifications messaging system.
 * 
 * @author kevin.off
 */
@Configuration
@EnableIntegration
@IntegrationComponentScan
@Import(NotificationConfiguration.class)
public class NotificationSenderConfiguration {
    
    @Autowired
    ConnectionFactory connectionFactory;
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;

    @Autowired
    RabbitClient rabbitClient;
    
    /**
     * Bean the represents the input channel where the messages flow to first to be routed to Rabbit.
     * 
     * @return The input channel reference
     */
    @Bean(name = "sendNotification.input")
    public PublishSubscribeChannel notificationSenderInput(){
        return MessageChannels.publishSubscribe("sendNotification.input", Executors.newCachedThreadPool()).get();
    }
    
    /**
     * A {@link MessagingGateway} interface that connects client code to the Spring Integration flow to Rabbit.
     */
    @MessagingGateway
    public interface NotificationSenderGateway {

        @Gateway(requestChannel = "sendNotification.input")
        public void sendNotification(Notification notification, @Header("BCC") List<String> bcc, @Header("Request-ID") String requestId, @Header("Correlation-ID") String correlationId);

    }
    
    /**
     * The actual integration flow object that receives messages on the input channel
     * and sends them to RabbitMQ.
     * 
     * @param applicationContext Param does not need to be here.
     * @return The Spring integration flow
     */
    @Bean
    public IntegrationFlow notificationSender(ConfigurableApplicationContext applicationContext){
        if (rabbitProps.getAppExchangeName() != null){
            return IntegrationFlows.from("sendNotification.input")
                    .handle(Amqp.outboundAdapter(catalogNotificationsTemplate())
                            .exchangeName(rabbitProps.getAppExchangeName())
                            .mappedRequestHeaders("*"))
                    .channel("nullChannel").get();
        }else{
            throw new RuntimeException("You must specify your app's sending exchange name 'datakow.rabbit.appExchangeName'");
        }
       
    }
    
    /**
     * The underlying RabbitTemplate DAO object used to perform actions on Rabbit.
     * 
     * @return The RabbitTemplate bean
     */
    @Bean
    public RabbitTemplate catalogNotificationsTemplate(){
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(notificationMessageConverter());
        return template;
    }
    
    /**
     * The message converter used to convert the notifications to JSON messages that are sent to Rabbit.
     * 
     * @return The converter
     */
    public Jackson2JsonMessageConverter notificationMessageConverter(){
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        DefaultClassMapper mapper = new DefaultClassMapper();
        mapper.setDefaultType(Notification.class);
        converter.setClassMapper(mapper);
        return converter;
    }
    
    /**
     * The client bean used to send messages.
     * <p>
     * The bean may be used by Autowiring it in any of your component classes
     * 
     * @return The client bean
     */
    @Bean
    public NotificationSenderClient notificationSenderClient(){
        return new NotificationSenderClient();
    }

    @Bean
    public TopicExchange declareEventServiceExchange(RabbitConfigurationProperties props) {
        TopicExchange exchange = new TopicExchange(props.getAppExchangeName(), true, false);
        return exchange;
    }

    @Bean
    public Binding declareEventServiceExchangeBinding(RabbitConfigurationProperties props) {
        Binding binding = new Binding("e.services.clients", DestinationType.EXCHANGE, props.getEventsExchangeName(), "#", null);
        return binding;
    }
    
}
