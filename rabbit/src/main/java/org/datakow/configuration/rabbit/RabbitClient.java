package org.datakow.configuration.rabbit;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

/**
 * The RabbitMQ Client used to access RabbitMQ
 * 
 * @author kevin.off
 */
public class RabbitClient {
    
    RabbitAdmin rabbitAdmin;
    
    /**
     * Creates an instance of the Client with the Rabbit Admin to access Rabbit
     * 
     * @param admin The underlying Rabbit Admin
     */
    public RabbitClient(RabbitAdmin admin){
        this.rabbitAdmin = admin;
    }
    
    /**
     * Creates a queue and binds it to an exchange using the supplied routing keys
     * 
     * @param queueName The name of the queue. Must begin with q.
     * @param exchangeName The name of the exchange to bind the queue to
     * @param autoDelete Whether the queue is autodelete or not
     * @param args Additional queue arguments
     * @param routingKeys Routing keys used to bind the queue
     * @return true on success
     */
    public boolean createAndBindQueue(String queueName, String exchangeName, boolean autoDelete, QueueArguments args, String ... routingKeys){
        
        //declare and bind to client queue
        Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "Declaring queue: {0}", queueName);
        String declaredQueueName = this.rabbitAdmin.declareQueue(new Queue(queueName, true, false, autoDelete, args.getArguments()));
        
        if (!declaredQueueName.equalsIgnoreCase(queueName)){
            Logger.getLogger(RabbitClient.class.getName()).log(Level.SEVERE, "Queue {0} could not be declared.", queueName);
            return false;
        }
        for(String routingKey : routingKeys){
            try{
                Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "Binding queue {0} to the {1} exchange with {2}", new Object[]{queueName, exchangeName, routingKey});
                this.rabbitAdmin.declareBinding(new Binding(queueName, Binding.DestinationType.QUEUE, exchangeName, routingKey, null));
            }catch (Exception e){
                deleteQueue(queueName);
                Logger.getLogger(RabbitClient.class.getName()).log(Level.SEVERE, "Could not declare binding for queue [" + queueName + "] to " + exchangeName + " with routing key + [" + routingKey + "].", e);
                return false;
            }
        }
        return true;
    }
    
    /**
     * Deletes a queue by its name
     * @param queueName The name of the queue by name
     * @return true on delete
     */
    public boolean deleteQueue(String queueName){
        Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "Deleting queue: {0}", queueName);
        boolean deleted = this.rabbitAdmin.deleteQueue(queueName);
        if (!deleted){
            Logger.getLogger(RabbitClient.class.getName()).log(Level.SEVERE, "The queue {0} could not be deleted.", queueName);
        }
        return deleted;
    }
    
    /**
     * Adds the queue to the listening container and starts it up if necessary
     * 
     * @param queueName the name of the queue to listen to
     * @param listenerContainer The container to listen to the queue with.
     * @return true on success
     */
    public boolean startListeningToQueue(String queueName, SimpleMessageListenerContainer listenerContainer){
        //add queue to listener container
        if (!Arrays.asList(listenerContainer.getQueueNames()).contains(queueName)) {
            String queueToRemove = "";
            //If the listener is not running but has 1 queue in it then it was shut down
            //when the last queue was going to be removed.
            //We need to remove the queue after we add the new one to make sure that it always has at least 1 queue in it.
            if (!listenerContainer.isRunning()) {
                if (listenerContainer.getQueueNames().length == 1) {
                    queueToRemove = listenerContainer.getQueueNames()[0];
                }
            }
            
            if (!Arrays.asList(listenerContainer.getQueueNames()).contains(queueName)){
                listenerContainer.addQueueNames(queueName);
                Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "Listening to queue [{0}]", queueName);
            }else{
                Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "Already listening to queue [{0}]", queueName);
            }
            
            if (!queueToRemove.isEmpty()) {
                listenerContainer.removeQueueNames(queueToRemove);
            }
        }else{
            Logger.getLogger(RabbitClient.class.getName()).log(Level.INFO, "The listener was already listeneing to queue [{0}]", queueName);
        }
        
        //start the container if it is not already running
        if (!listenerContainer.isRunning()) {
            Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "The listener container was asleep. Waking it up.");
            listenerContainer.start();
        }
        return true;
    }
    
    /**
     * Stop listening to a queue and stop the listener if it was the last queue.
     * 
     * @param queueName The name of the queue
     * @param listenerContainer The listener container that is listening
     * @return true on success
     */
    public boolean stopListeningToQueue(String queueName, SimpleMessageListenerContainer listenerContainer) {
        if (Arrays.asList(listenerContainer.getQueueNames()).contains(queueName)) {
            //If this is the last queue in the listener then we must stop the listener because it can't tolorate having no queues
            if (listenerContainer.getQueueNames().length == 1) {
                Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "The queue [{0}] is the last queue on the container so we are going to stop listening to RabbitMQ.", queueName);
                listenerContainer.stop();
            } else {
                //If there are other queues in the listener then we can remove the queue from the container
                Logger.getLogger(RabbitClient.class.getName()).log(Level.FINE, "Removing queue [{0}] from the listener", queueName);
                boolean removed = listenerContainer.removeQueueNames(queueName);
                if (!removed){
                    Logger.getLogger(RabbitClient.class.getName()).log(Level.SEVERE, "The queue [{0}] was not able to be removed from the listener", queueName);
                }
            }
        } else {
            Logger.getLogger(RabbitClient.class.getName()).log(Level.SEVERE, "The queue [{0}] was not being listened to in the listener. If the queue exists it is still on RabbitMQ.", queueName);
        }
        return true;
    }
    
    /**
     * Gets the size of the queue or null if the queue does not exist.
     * 
     * @param queueName The name of the queue
     * @return The number of messages in the queue or null if the queue does not exist
     */
    public Integer getQueueSize(String queueName){
        
        Properties props = rabbitAdmin.getQueueProperties(queueName);
        if (props != null){
            return (Integer)props.get("QUEUE_MESSAGE_COUNT");
        }
        return null;
    }
    
}
