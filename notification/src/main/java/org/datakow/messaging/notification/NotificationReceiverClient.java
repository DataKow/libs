package org.datakow.messaging.notification;


import org.datakow.configuration.rabbit.QueueArguments;
import org.datakow.configuration.rabbit.RabbitClient;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The client bean used to configure the receiving end of the messages.
 * <p>
 * Users should first implement their NotificationReceiver instances if you plan on receiving notifications.
 * Then this class will be available as a bean of type NotificationReceiverClient.
 */
public class NotificationReceiverClient {
    
    private final String servicesClientsExchangeName;
    
    @Autowired
    private RabbitClient rabbitClient;
        
    private final SimpleMessageListenerContainer clientListenerContainer;
    
    /**
     * Creates a new client with all of the components needed.
     * 
     * @param container The listener container used to listen to queues
     * @param servicesClientsExchangeName The name of the exchange that is being listened to
     */
    public NotificationReceiverClient(SimpleMessageListenerContainer container, String servicesClientsExchangeName){
        this.clientListenerContainer = container;
        this.servicesClientsExchangeName = servicesClientsExchangeName;
    }
    
    /**
     * Starts listening to notifications for a given subscription
     * 
     * @param subscriptionId The id of the subscription
     * @return true on success
     */
    public boolean startListeningToNotifications(String subscriptionId){
        String queueName = "q.subscriber." + subscriptionId;
        return rabbitClient.startListeningToQueue(queueName, clientListenerContainer);
    }
    
    /**
     * Stops listening to notifications for a given subscription
     * 
     * @param subscriptionId The ID of the subscription
     * @return true on success
     */
    public boolean stopListeningToNotifications(String subscriptionId){
        String queueName = "q.subscriber." + subscriptionId;
        return rabbitClient.stopListeningToQueue(queueName, clientListenerContainer);
    }
    
    /**
     * Creates and binds a subscription queue to the services clients exchange
     * 
     * @param subscriptionId The id of the subscription
     * @return true on success
     */
    public boolean createAndBindQueue(String subscriptionId){
        return createAndBindQueue(subscriptionId, QueueArguments.defaultArguments());
    }
    
    /**
     * Creates and binds a subscription queue to the services clients exchange
     * 
     * @param subscriptionId The id of the subscription
     * @param args Queue arguments used to create the queue
     * @return true on success
     */
    public boolean createAndBindQueue(String subscriptionId, QueueArguments args){
        String queueName = "q.subscriber." + subscriptionId;
        return rabbitClient.createAndBindQueue(queueName, servicesClientsExchangeName, false, args, queueName);
    }
    
    /**
     * Deletes a queue for a given subscription
     * 
     * @param subscriptionId The subscription id
     * @return true on success
     */
    public boolean deleteQueue(String subscriptionId){
        
        String queueName = "q.subscriber." + subscriptionId;
        return rabbitClient.deleteQueue(queueName);
    }
}