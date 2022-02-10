package org.datakow.configuration.rabbit.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties used to establish a connection to the RabbitMQ server.
 * These properties need to exist in a properties file or on the config server.
 * 
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.rabbit")
public class RabbitConfigurationProperties {

    private String eventsExchangeName;
    private String servicesClientsExchangeName;
    private String servicesExchangeName;
    private String appExchangeName;
    private String appId;
    private String hostName;
    private int port = 5672;
    private String password;
    private String virtualHost = "datakow";
    private int channelCacheSize = 5;
    private long channelCheckoutTimeout = 30000;
    private int notificationsPrefetchCount = 10;
    private int eventsPrefetchCount = 10;
    private String messagingVersion;

    /**
     * Gets the application's default app exchange name used to send messages.
     * 
     * @return The name of the default exchange
     */
    public String getAppExchangeName() {
        return appExchangeName;
    }

    /**
     * Sets the application's default app exchange name used to send messages.
     * 
     * @param appExchangeName The name of the default exchange
     */
    public void setAppExchangeName(String appExchangeName) {
        this.appExchangeName = appExchangeName;
    }

    /**
     * Get the username used to access RabbitMQ
     * 
     * @return The username
     */
    public String getAppId() {
        return appId;
    }

    /**
     * Set the username used to access RabbitMQ
     * 
     * @param appId The username
     */
    public void setAppId(String appId) {
        this.appId = appId;
    }

    /**
     * Gets the number of channels to cache
     * 
     * @return The number of cached channels
     */
    public int getChannelCacheSize() {
        return channelCacheSize;
    }

    /**
     * Sets the number of channels to cache
     * 
     * @param channelCacheSize The number of cached channels
     */
    public void setChannelCacheSize(int channelCacheSize) {
        this.channelCacheSize = channelCacheSize;
    }

    /**
     * Gets the amount of time to wait for a channel before exception
     * 
     * @return The amount of time in milliseconds 
     */
    public long getChannelCheckoutTimeout() {
        return channelCheckoutTimeout;
    }

    /**
     * Sets the amount of time to wait for a channel before exception
     * 
     * @param channelCheckoutTimeout The amount of time in milliseconds 
     */
    public void setChannelCheckoutTimeout(long channelCheckoutTimeout) {
        this.channelCheckoutTimeout = channelCheckoutTimeout;
    }

    /**
     * Get the name of the events exchange
     * 
     * @return The name of the events exchange 
     */
    public String getEventsExchangeName() {
        return eventsExchangeName;
    }

    /**
     * Set the name of the events exchange
     * 
     * @param eventsExchangename The name of the events exchange 
     */
    public void setEventsExchangeName(String eventsExchangename) {
        this.eventsExchangeName = eventsExchangename;
    }

    /**
     * Get the name of the services clients exchange
     * 
     * @return The name of the events exchange 
     */
    public String getServicesClientsExchangeName() {
        return servicesClientsExchangeName;
    }

    /**
     * Set the name of the services clients exchange
     * 
     * @param servicesClientsExchangeName The name of the services clients exchange 
     */
    public void setServicesClientsExchangeName(String servicesClientsExchangeName) {
        this.servicesClientsExchangeName = servicesClientsExchangeName;
    }

    /**
     * Get the name of the services exchange
     * 
     * @return The name of the services exchange 
     */
    public String getServicesExchangeName() {
        return servicesExchangeName;
    }

    /**
     * Set the name of the services exchange
     * 
     * @param servicesExchangeName The name of the services exchange 
     */
    public void setServicesExchangeName(String servicesExchangeName) {
        this.servicesExchangeName = servicesExchangeName;
    }

    /**
     * Gets the host name of the RabbitMQ server
     * 
     * @return The host name
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Sets the host name of the RabbitMQ server
     * 
     * @param hostName The host name
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Get the port to connect to Rabbit
     * 
     * @return The port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the port to connect to Rabbit
     * 
     * @param port The port number
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets the password used to connect to RabbitMQ
     * 
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password used to connect to RabbitMQ
     * 
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets the virtual host to use
     * 
     * @return The virtual host
     */
    public String getVirtualHost() {
        return virtualHost;
    }

    /**
     * Sets the virtual host to use
     * 
     * @param virtualHost The virtual host
     */
    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    /**
     * Gets the number of notifications to prefetch
     * 
     * @return The number of notifications to prefetch
     */
    public int getNotificationsPrefetchCount() {
        return notificationsPrefetchCount;
    }

    /**
     * Sets the number of notifications to prefetch
     * 
     * @param notificationsPrefetchCount The number of notifications to prefetch
     */
    public void setNotificationsPrefetchCount(int notificationsPrefetchCount) {
        this.notificationsPrefetchCount = notificationsPrefetchCount;
    }

    /**
     * Gets the number of events to prefetch
     * 
     * @return The number of events to prefetch
     */
    public int getEventsPrefetchCount() {
        return eventsPrefetchCount;
    }

    /**
     * Sets the number of events to prefetch
     * 
     * @param eventsPrefetchCount The number of events to prefetch
     */
    public void setEventsPrefetchCount(int eventsPrefetchCount) {
        this.eventsPrefetchCount = eventsPrefetchCount;
    }

    /**
     * Get the version of the DATAKOW messaging system to use
     * 
     * @return The messaging version
     */
    public String getMessagingVersion() {
        return messagingVersion;
    }

    /**
     * Set the version of the DATAKOW messaging system to use
     * 
     * @param messagingVersion The messaging version
     */
    public void setMessagingVersion(String messagingVersion) {
        this.messagingVersion = messagingVersion;
    }

    
    
}
