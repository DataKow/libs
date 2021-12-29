package org.datakow.catalogs.subscription.webservice.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties class used to inject properties found in the applications
 * property file or from the config server.
 * <p>
 * The properties mapped to this file will have a prefix of: datakow.subscription
 * 
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.subscription")
public class SubscriptionConfigurationProperties {
    
    private String webserviceHost;
    private int webservicePort;
    private String webserviceUsername;
    private String webservicePassword;

    /**
     * Gets the Subscription Web Service's hostname
     * 
     * @return The host of the subscription webservice
     */
    public String getWebserviceHost() {
        return webserviceHost;
    }

    /**
     * Gets the Subscription Web Service's port
     * 
     * @return The port of the subscription webservice
     */
    public int getWebservicePort() {
        return webservicePort;
    }

    /**
     * Sets the Subscription Web Service's host
     * 
     * @param webserviceHost The host of the subscription web service
     */
    public void setWebserviceHost(String webserviceHost) {
        this.webserviceHost = webserviceHost;
    }

    /**
     * Sets the Subscription Web Service's port
     * 
     * @param webservicePort The port of the subscription web service
     */
    public void setWebservicePort(int webservicePort) {
        this.webservicePort = webservicePort;
    }

    /**
     * Gets the username used to interact with the Subscription Web Service
     * 
     * @return The username
     */
    public String getWebserviceUsername() {
        return webserviceUsername;
    }

    /**
     * Sets the username used to interact with the Subscription Web Service
     * 
     * @param webserviceUsername The username
     */
    public void setWebserviceUsername(String webserviceUsername) {
        this.webserviceUsername = webserviceUsername;
    }

    /**
     * Gets the password used to interact with the Subscription Web Service
     * 
     * @return The password
     */
    public String getWebservicePassword() {
        return webservicePassword;
    }

    /**
     * Sets the password used to interact with the Subscription Web Service
     * 
     * @param webservicePassword The password
     */
    public void setWebservicePassword(String webservicePassword) {
        this.webservicePassword = webservicePassword;
    }
 
    
    
}
