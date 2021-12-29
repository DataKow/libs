package org.datakow.messaging.events.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties used to configure event receiver clients.
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.rabbit.events.receiver")
public class EventReceiverConfigurationProperties {
    
    private int concurrentConsumers = 1;

    /**
     * Get the number of concurrent consumers that are consuming events.
     * 
     * @return The number of consumers
     */
    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }

    /**
     * Set the number of concurrent consumers that are consuming events.
     * 
     * @param concurrentConsumers The number of consumers
     */
    public void setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
    }
    
}
