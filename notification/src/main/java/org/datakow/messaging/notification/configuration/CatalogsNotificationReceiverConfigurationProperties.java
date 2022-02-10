package org.datakow.messaging.notification.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties used to configure notification receiver clients.
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.rabbit.notifications.receiver")
public class CatalogsNotificationReceiverConfigurationProperties {

    private int concurrentConsumers = 1;

    /**
     * Get the number of concurrent consumers that are consuming notifications.
     * 
     * @return The number of consumers
     */
    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }

    /**
     * Set the number of concurrent consumers that are consuming notifications.
     * 
     * @param concurrentConsumers The number of consumers
     */
    public void setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
    }
    
}
