package org.datakow.catalogs.object.webservice.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties to use to setup the Object Catalog Web Service Client
 * 
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.catalogs")
public class ObjectCatalogWebServiceClientConfigurationProperties {
    
    private String objectCatalogWebserviceHost;
    private int objectCatalogWebservicePort;
    private String metadataCatalogWebserviceHost;
    private int metadataCatalogWebservicePort;
    private int objectCatalogWebserviceClientConnectTimeout;
    private int objectCatalogWebserviceClientReadTimeout;
    private int metadataCatalogWebserviceClientConnectTimeout;
    private int metadataCatalogWebserviceClientReadTimeout;
    private String webserviceUsername;
    private String webservicePassword;
    private int catalogRegistryCacheTimeInMinutes;
    private boolean catalogRegistryIncludeIndexes;
    private int metadataCatalogWebserviceClientMaxTotalConnections = -1;
    private int metadataCatalogWebserviceClientMaxTotalConnectionsPerRoute = -1;
    
    /**
     * Gets the Hostname for the object catalog web service.
     * This includes the path up to the web service version.
     * 
     * @return the hostname of the object catalog web service
     */
    public String getObjectCatalogWebserviceHost() {
        return objectCatalogWebserviceHost;
    }

    /**
     * Gets the port for the object catalog web service.
     * This includes the path up to the web service version.
     * 
     * @return the port of the object catalog web service
     */
    public int getObjectCatalogWebservicePort() {
        return objectCatalogWebservicePort;
    }

    /**
     * Sets the hostname for the object catalog web service.
     * This includes the path up to the web service version.
     * 
     * @param objectCatalogWebserviceHost the hostname of the object catalog web service
     */
    public void setObjectCatalogWebserviceHost(String objectCatalogWebserviceHost) {
        this.objectCatalogWebserviceHost = objectCatalogWebserviceHost;
    }

    /**
     * Sets the port for the object catalog web service.
     * This includes the path up to the web service version.
     * 
     * @param objectCatalogWebservicePort the port of the object catalog web service
     */
    public void setObjectCatalogWebservicePort(int objectCatalogWebservicePort) {
        this.objectCatalogWebservicePort = objectCatalogWebservicePort;
    }

    /**
     * Gets the host for the metadata catalog web service.
     * This includes the path up to the web service version.
     * 
     * @return the host of the metadata catalog web service
     */
    public String getMetadataCatalogWebserviceHost() {
        return metadataCatalogWebserviceHost;
    }

    /**
     * Gets the port for the metadata catalog web service.
     * This includes the path up to the web service version.
     * 
     * @return the port of the metadata catalog web service
     */
    public int getMetadataCatalogWebservicePort() {
        return metadataCatalogWebservicePort;
    }

    /**
     * Sets the base URL for the metadata catalog web service.
     * This includes the path up to the web service version.
     * 
     * @param metadataCatalogWebserviceHost the host of the metadata catalog web service
     */
    public void setMetadataCatalogWebserviceHost(String metadataCatalogWebserviceHost) {
        this.metadataCatalogWebserviceHost = metadataCatalogWebserviceHost;
    }

    /**
     * Sets the port for the metadata catalog web service.
     * This includes the path up to the web service version.
     * 
     * @param metadataCatalogWebservicePort the port of the metadata catalog web service
     */
    public void setMetadataCatalogWebservicePort(int metadataCatalogWebservicePort) {
        this.metadataCatalogWebservicePort = metadataCatalogWebservicePort;
    }

    /**
     * Gets the connect timeout limit in milliseconds to establish a connection
     * 
     * @return The connect timeout in milliseconds
     */
    public int getObjectCatalogWebserviceClientConnectTimeout() {
        return objectCatalogWebserviceClientConnectTimeout;
    }

    /**
     * Sets the connect timeout limit in milliseconds to establish a connection
     * 
     * @param objectCatalogWebserviceClientConnectTimeout The connect timeout in milliseconds
     */
    public void setObjectCatalogWebserviceClientConnectTimeout(int objectCatalogWebserviceClientConnectTimeout) {
        this.objectCatalogWebserviceClientConnectTimeout = objectCatalogWebserviceClientConnectTimeout;
    }

    /**
     * Gets the amount of time in milliseconds that a request is allowed to read a response before a timeout exception.
     * 
     * @return The read timeout in milliseconds
     */
    public int getObjectCatalogWebserviceClientReadTimeout() {
        return objectCatalogWebserviceClientReadTimeout;
    }

    /**
     * Sets the amount of time in milliseconds that a request is allowed to read a response before a timeout exception.
     * 
     * @param objectCatalogWebserviceClientReadTimeout The read timeout in milliseconds
     */
    public void setObjectCatalogWebserviceClientReadTimeout(int objectCatalogWebserviceClientReadTimeout) {
        this.objectCatalogWebserviceClientReadTimeout = objectCatalogWebserviceClientReadTimeout;
    }

    /**
     * Gets the connect timeout limit in milliseconds to establish a connection
     * 
     * @return The connect timeout in milliseconds
     */
    public int getMetadataCatalogWebserviceClientConnectTimeout() {
        return metadataCatalogWebserviceClientConnectTimeout;
    }

    /**
     * Sets the connect timeout limit in milliseconds to establish a connection
     * 
     * @param metadataCatalogWebserviceClientConnectTimeout The connect timeout in milliseconds
     */
    public void setMetadataCatalogWebserviceClientConnectTimeout(int metadataCatalogWebserviceClientConnectTimeout) {
        this.metadataCatalogWebserviceClientConnectTimeout = metadataCatalogWebserviceClientConnectTimeout;
    }

    /**
     * Gets the amount of time in milliseconds that a request is allowed to read a response before a timeout exception.
     * 
     * @return The read timeout in milliseconds
     */
    public int getMetadataCatalogWebserviceClientReadTimeout() {
        return metadataCatalogWebserviceClientReadTimeout;
    }

    /**
     * Sets the amount of time in milliseconds that a request is allowed to read a response before a timeout exception.
     * 
     * @param metadataCatalogWebserviceClientReadTimeout The read timeout in milliseconds
     */
    public void setMetadataCatalogWebserviceClientReadTimeout(int metadataCatalogWebserviceClientReadTimeout) {
        this.metadataCatalogWebserviceClientReadTimeout = metadataCatalogWebserviceClientReadTimeout;
    }

    /**
     * Gets the username used to access the catalog
     * 
     * @return The username
     */
    public String getWebserviceUsername() {
        return webserviceUsername;
    }

    /**
     * Sets the username used to access the catalog
     * 
     * @param webserviceUsername The username
     */
    public void setWebserviceUsername(String webserviceUsername) {
        this.webserviceUsername = webserviceUsername;
    }

    /**
     * Gets the password used to access the catalog
     * 
     * @return The password
     */
    public String getWebservicePassword() {
        return webservicePassword;
    }

    /**
     * Sets the password used to access the catalog
     * 
     * @param webservicePassword The password
     */
    public void setWebservicePassword(String webservicePassword) {
        this.webservicePassword = webservicePassword;
    }

    /**
     * Gets the time in minutes to cache the catalogs in the CatalogRegistry
     * 
     * @return The cache time in minutes
     */
    public int getCatalogRegistryCacheTimeInMinutes() {
        return catalogRegistryCacheTimeInMinutes;
    }

    /**
     * Sets the time in minutes to cache the catalogs in the CatalogRegistry
     * 
     * @param catalogRegistryCacheTimeInMinutes The cache time in minutes
     */
    public void setCatalogRegistryCacheTimeInMinutes(int catalogRegistryCacheTimeInMinutes) {
        this.catalogRegistryCacheTimeInMinutes = catalogRegistryCacheTimeInMinutes;
    }

    /**
     * Returns true if the CatalogRegistry is configured to include indexes in the cache.
     * 
     * @return true if it is to include indexes
     */
    public boolean isCatalogRegistryIncludeIndexes() {
        return catalogRegistryIncludeIndexes;
    }

    /**
     * If the CatalogRegistry is configured to include indexes in the cache.
     * 
     * @param catalogRegistryIncludeIndexes true if it is to include indexes
     */
    public void setCatalogRegistryIncludeIndexes(boolean catalogRegistryIncludeIndexes) {
        this.catalogRegistryIncludeIndexes = catalogRegistryIncludeIndexes;
    }

    /**
     * Gets the max number of connections that the RestTemplate can have at one time.
     * 
     * @return The max number of total connections
     */
    public int getMetadataCatalogWebserviceClientMaxTotalConnections() {
        return metadataCatalogWebserviceClientMaxTotalConnections;
    }

    /**
     * Sets the max number of connections that the RestTemplate can have at one time.
     * 
     * @param metadataCatalogWebserviceClientMaxTotalConnections The max number of total connections
     */
    public void setMetadataCatalogWebserviceClientMaxTotalConnections(int metadataCatalogWebserviceClientMaxTotalConnections) {
        this.metadataCatalogWebserviceClientMaxTotalConnections = metadataCatalogWebserviceClientMaxTotalConnections;
    }

    /**
     * Gets the max number of connections that the RestTemplate can have at one time to any one route.
     * 
     * @return The max number of total connections
     */
    public int getMetadataCatalogWebserviceClientMaxTotalConnectionsPerRoute() {
        return metadataCatalogWebserviceClientMaxTotalConnectionsPerRoute;
    }

    /**
     * Sets the max number of connections that the RestTemplate can have at one time to any one route.
     * 
     * @param metadataCatalogWebserviceClientMaxTotalConnectionsPerRoute The max number of total connections
     */
    public void setMetadataCatalogWebserviceClientMaxTotalConnectionsPerRoute(int metadataCatalogWebserviceClientMaxTotalConnectionsPerRoute) {
        this.metadataCatalogWebserviceClientMaxTotalConnectionsPerRoute = metadataCatalogWebserviceClientMaxTotalConnectionsPerRoute;
    }
    
}
