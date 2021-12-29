package org.datakow.configuration.rabbit.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the exclusive lock feature.
 * 
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.rabbit.lock")
public class ExclusiveLockConfigurationProperties {
    
    private String lockName;
    private String lockAcquisionAttemptDelayInMs;

    /**
     * Gets the name of the lock that is used to create the exclusive queue.
     * 
     * @return The name of the lock
     */
    public String getLockName() {
        return lockName;
    }

    /**
     * Sets the name of the lock that is used to create the exclusive queue.
     * 
     * @param lockName The name of the lock
     */
    public void setLockName(String lockName) {
        this.lockName = lockName;
    }

    /**
     * Gets the delay in milliseconds in between attempts to acquire the lock.
     * 
     * @return The delay in milliseconds
     */
    public String getLockAcquisionAttemptDelayInMs() {
        return lockAcquisionAttemptDelayInMs;
    }

    /**
     * Gets the delay in milliseconds in between attempts to acquire the lock.
     * 
     * @param lockAcquisionAttemptDelayInMs The delay in milliseconds
     */
    public void setLockAcquisionAttemptDelayInMs(String lockAcquisionAttemptDelayInMs) {
        this.lockAcquisionAttemptDelayInMs = lockAcquisionAttemptDelayInMs;
    }
    
    
    
}
