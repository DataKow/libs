package org.datakow.configuration.rabbit;

import java.util.HashMap;
import java.util.Map;

/**
 * Queue arguments to pass to Rabbit MQ when creating your queue for consumption
 * 
 * @author kevin.off
 */
public class QueueArguments {
    
    private final Map<String, Object> arguments = new HashMap<>();

    /**
     * Set an argument by name
     * 
     * @param argumentName The argument name
     * @param argumentValue The argument value
     */
    public void setArgument(String argumentName, Object argumentValue){
        this.arguments.put(argumentName, argumentValue);
    }
    
    /**
     * Gets the argument by name
     * 
     * @param <T> The expected return type
     * @param argumentName The name of the argument
     * @return The value of the argument
     */
    public <T> T getArgument(String argumentName){
        return (T)this.arguments.get(argumentName);
    }
    
    /**
     * Gets a map of all arguments or null
     * 
     * @return all arguments 
     */
    public Map<String, Object> getArguments(){
        return this.arguments.isEmpty() ? null : this.arguments;
    }
    
    /**
     * How long a message published to a queue can live before it is discarded (milliseconds).
     * @return The TTL
     */
    public int getMessageTTL() {
        return this.getArgument("x-message-ttl");
    }

    /**
     * How long a message published to a queue can live before it is discarded (milliseconds).
     * @param messageTTL The TTL
     */
    public void setMessageTTL(int messageTTL) {
        this.setArgument("x-message-ttl", messageTTL);
    }

    /**
     * How long a queue can be unused for before it is automatically deleted (milliseconds).
     * @return expire time
     */
    public int getExpires() {
        return this.getArgument("x-expires");
    }

    /**
     * How long a queue can be unused for before it is automatically deleted (milliseconds).
     * @param expires expire time
     */
    public void setExpires(int expires) {
        this.setArgument("x-expires", expires);
    }

    /**
     * How many (ready) messages a queue can contain before it starts to drop them from its head.
     * @return max length
     */
    public int getMaxLength() {
        return this.getArgument("x-max-length");
    }

    /**
     * How many (ready) messages a queue can contain before it starts to drop them from its head.
     * @param maxLength max length
     */
    public void setMaxLength(int maxLength) {
        this.setArgument("x-max-length", maxLength);
    }

    /**
     * Total body size for ready messages a queue can contain before it starts to drop them from its head.
     * @return max length in bytes
     */
    public int getMaxLengthBytes() {
        return this.getArgument("x-max-length-bytes");
    }

    /**
     * Total body size for ready messages a queue can contain before it starts to drop them from its head.
     * @param maxLengthBytes max length in bytes
     */
    public void setMaxLengthBytes(int maxLengthBytes) {
        this.setArgument("x-max-length-bytes", maxLengthBytes);
    }

    /**
     * Optional name of an exchange to which messages will be republished if they are rejected or expire.
     * @return dead letter exchange
     */
    public String getDeadLetterExchange() {
        return this.getArgument("x-dead-letter-exchange");
    }

    /**
     * Optional name of an exchange to which messages will be republished if they are rejected or expire.
     * @param deadLetterExchange dead letter exchange
     */
    public void setDeadLetterExchange(String deadLetterExchange) {
        this.setArgument("x-dead-letter-exchange", deadLetterExchange);
    }

    /**
     * Optional replacement routing key to use when a message is dead-lettered. 
     * If this is not set, the message's original routing key will be used.
     * @return dead letter exchange routing key
     */
    public String getDeadLetterExchangeRoutingKey() {
        return this.getArgument("x-dead-letter-routing-key");
    }

    /**
     * Optional replacement routing key to use when a message is dead-lettered. 
     * If this is not set, the message's original routing key will be used.
     * @param deadLetterExchangeRoutingKey dead letter exchange routing key
     */
    public void setDeadLetterExchangeRoutingKey(String deadLetterExchangeRoutingKey) {
        this.setArgument("x-dead-letter-routing-key", deadLetterExchangeRoutingKey);
    }

    /**
     * Maximum number of priority levels for the queue to support.
     * if not set, the queue will not support message priorities.
     * @return max priority
     */
    public int getMaxPriority() {
        return this.getArgument("x-max-priority");
    }

    /**
     * Maximum number of priority levels for the queue to support.
     * if not set, the queue will not support message priorities.
     * @param maxPriority max priority
     */
    public void setMaxPriority(int maxPriority) {
        this.setArgument("x-max-priority", maxPriority);
    }
    
    /**
     * Get the default queue arguments that DATAKOW uses on its client queues
     * @return The default arguments
     */
    public static QueueArguments defaultArguments(){
        QueueArguments args = new QueueArguments();
        args.setMessageTTL(10800000);
        return args;
    }
    
}
