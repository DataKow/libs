package org.datakow.messaging.events;


import org.datakow.configuration.rabbit.QueueArguments;
import org.datakow.configuration.rabbit.RabbitClient;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The client bean used to configure the receiving end of the messages.
 * <p>
 * Users should first implement their NotificationReceiver instances if you plan on receiving notifications.
 * Then this class will be available as a bean of type CatalogEventsReceiverClient.
 */
public class CatalogEventsReceiverClient {
    
    private final String eventsExchangeName;
    private final String applicationName;
    private final String messagingVersion;
    private final String myGuid = UUID.randomUUID().toString();
    
    @Autowired
    private EventsRoutingKeyBuilder routingKeyBuilder;
    
    @Autowired
    private RabbitClient rabbitClient;
    
    private final SimpleMessageListenerContainer clientListenerContainer;
    
    /**
     * Creates a new client with all of the components needed.
     * 
     * @param container The listener container used to listen to queues
     * @param exchangeName The name of the exchange that is being listened to
     * @param applicationName The name of the application used in the queue name
     * @param messagingVersion The DATAKOW messaging version used in the queue name and routing key
     */
    public CatalogEventsReceiverClient(
        SimpleMessageListenerContainer container, 
        String exchangeName, 
        String applicationName, 
        String messagingVersion){
            
        this.clientListenerContainer = container;
        this.eventsExchangeName = exchangeName;
        this.applicationName = applicationName;
        this.messagingVersion = messagingVersion;
    }
    
    /**
     * Creates the name of the queue to listen to.
     * 
     * @param queueIdentifier The unique identifier to use in the queue
     * @return The name of the queue
     */
    private String queueName(String queueIdentifier){
        return "q." + applicationName + ".catalog.events." + queueIdentifier;
    }
    
    /**
     * Starts receiving events for the provided event types on a queue where all 
     * applications of the same name will share the same queue.
     * <p>
     * All applications will receive events in round-robin
     * 
     * @param eventTypes All of the event types to listen to
     * @return The name of the queue that is being listened to
     */
    public String startReceivingEventsShared(String ... eventTypes){
        return startReceivingEventsShared(QueueArguments.defaultArguments(), eventTypes);
    }
    
    /**
     * Starts receiving events for the provided event types on a queue where all instances
     * applications of the same name will share the same queue.
     * <p>
     * All applications will receive events in round-robin
     * 
     * @param args The queue arguments used to create the queue
     * @param eventTypes All of the event types to listen to
     * @return The name of the queue that is being listened to
     */
    public String startReceivingEventsShared(QueueArguments args, String ... eventTypes){
        String queueName = queueName(messagingVersion);
        String[] routingKeys = new String[eventTypes.length];
        int i = 0;
        for (String eventType : eventTypes){
           routingKeys[i] = (routingKeyBuilder.buildRoutingKey("*", eventType, "*", "*", "*"));
           i++;
        }
        boolean created = rabbitClient.createAndBindQueue(queueName, eventsExchangeName, false, args, routingKeys);
        boolean listening = rabbitClient.startListeningToQueue(queueName, clientListenerContainer);
        if (created && listening){
            return queueName;
        }else{
            throw new RuntimeException("Could not create, bind, and listen to queue " + queueName);
        }
    }
    
    /**
     * Starts receiving events for the provided event types on a queue where all instances of 
     * applications will receive their own queue.
     * <p>
     * All applications will receive a copy of all events
     * 
     * @param eventTypes All of the event types to listen to
     * @return The name of the queue that is being listened to
     */
    public String startReceivingEventsIndividual(String ... eventTypes){
        return startReceivingEventsIndividual(QueueArguments.defaultArguments(), eventTypes);
    }
    
    /**
     * Starts receiving events for the provided event types on a queue where all instances of 
     * applications will receive their own queue.
     * <p>
     * All applications will receive a copy of all events
     * 
     * @param args Queue arguments used when creating the queue
     * @param eventTypes All of the event types to listen to
     * @return The name of the queue that is being listened to
     */
    public String startReceivingEventsIndividual(QueueArguments args, String ... eventTypes){
        String queueName = queueName(myGuid);
        String[] routingKeys = new String[eventTypes.length];
        int i = 0;
        for (String eventType : eventTypes){
           routingKeys[i] = routingKeyBuilder.buildRoutingKey("*", eventType, "*", "*", "*");
           i++;
        }
        boolean created = rabbitClient.createAndBindQueue(queueName, eventsExchangeName, true, args, routingKeys);
        boolean listening = rabbitClient.startListeningToQueue(queueName, clientListenerContainer);
        if (created && listening){
            return queueName;
        }else{
            throw new RuntimeException("Could not create, bind, and listen to queue " + queueName);
        }
    }
    
    /**
     * Starts receiving events for the given event type and service name on a queue where all instances of 
     * all applications will receive their own queue.
     * 
     * @param serviceName The name of the service that produced the event
     * @param eventType The event Type
     * @return The name of the queue
     */
    public String startReceivingEventsIndividualService(String serviceName, String eventType){
        QueueArguments args = QueueArguments.defaultArguments();
        String queueName = queueName(myGuid);
        String routingKey = routingKeyBuilder.buildRoutingKey(serviceName, eventType, "*", "*", "*");
        boolean created = rabbitClient.createAndBindQueue(queueName, eventsExchangeName, true, args, routingKey);
        boolean listening = rabbitClient.startListeningToQueue(queueName, clientListenerContainer);
        if (created && listening){
            return queueName;
        }else{
            throw new RuntimeException("Could not create, bind, and listen to queue " + queueName);
        }
    }
    
    /**
     * Starts receiving events for the provided event type on a queue where all instances
     * of the same service name will share the same queue.
     * 
     * @param serviceName the name of the service that produced the event
     * @param eventType The event Type
     * @return The name of the queue
     */
    public String startReceivingEventsSharedService(String serviceName, String eventType){
        QueueArguments args = QueueArguments.defaultArguments();
        String queueName = queueName(serviceName + "." + messagingVersion);
        String routingKey = routingKeyBuilder.buildRoutingKey(serviceName, eventType, "*", "*", "*");
        boolean created = rabbitClient.createAndBindQueue(queueName, eventsExchangeName, false, args, routingKey);
        boolean listening = rabbitClient.startListeningToQueue(queueName, clientListenerContainer);
        if (created && listening){
            return queueName;
        }else{
            throw new RuntimeException("Could not create, bind, and listen to queue " + queueName);
        }
    }
    
    /**
     * Stops listening to events for a given queue
     * 
     * @param queueName The name of the queue to stop listening to
     * @return True on success
     */
    public boolean stopReceiveingEvents(String queueName){
        boolean success = rabbitClient.stopListeningToQueue(queueName, clientListenerContainer);
        success = success && rabbitClient.deleteQueue(queueName);
        return success;
    }
    
    /**
     * Stops listening to all events that were registered to the event listener.
     * 
     * @return True on success
     */
    public boolean stopReceivingAllEvents(){
        boolean success = true;
        for(String queueName : clientListenerContainer.getQueueNames()){
            success = success && rabbitClient.stopListeningToQueue(queueName, clientListenerContainer);
            success = success && rabbitClient.deleteQueue(queueName);
        }
        return success;
    }
    
    /**
     * Gets the Spring listener container that is responsible for listening to queues.
     * 
     * @return The container
     */
    public SimpleMessageListenerContainer getClientListenerContainer(){
        return this.clientListenerContainer;
    }
    
    /**
     * Gets all queue names that have been registered to the listener container
     * 
     * @return The names of the queues
     */
    public List<String> getActiveQueueNames(){
        return Arrays.asList(clientListenerContainer.getQueueNames());
    }
}
