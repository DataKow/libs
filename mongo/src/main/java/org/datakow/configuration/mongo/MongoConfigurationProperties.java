package org.datakow.configuration.mongo;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties used to configure the connection to MongoDB.
 * 
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.mongo")
public class MongoConfigurationProperties {

    private String databaseName;
    
    private String objectCatalogCollectionName;
    
    private String servers;
    
    private int port;
    
    private int connPerHost;
    
    private int threadBlockMultiplier;
    
    private String userName;
    
    private String password;
    
    private boolean useAuth = true;
    
    private String readPreference = "PREFER_SECONDARY";
    
    private ReadPreference mongoReadPreference = ReadPreference.secondaryPreferred();
    
    private String writeConcern = "MAJORITY";
    
    private WriteConcern mongoWriteConcern = WriteConcern.MAJORITY;

    /**
     * Gets name of the database to use
     * 
     * @return The database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets name of the database to use
     * 
     * @param databaseName The database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Sets whether to use authentication to login to the server
     * 
     * @param auth true to use authentication 
     */
    public void setUseAuth(boolean auth){
        this.useAuth = auth;
    }
    
    /**
     * Gets whether to use authentication to login to the server
     * 
     * @return true to use authentication 
     */
    public boolean getUseAuth(){
        return this.useAuth;
    }
    
    /**
     * Gets the default object catalog collection name (bucket name)
     * 
     * @return The collection name
     */
    public String getObjectCatalogCollectionName() {
        return objectCatalogCollectionName;
    }

    /**
     *Sets the default object catalog collection name (bucket name)
     * 
     * @param objectCatalogCollectionName The collection name
     */
    public void setObjectCatalogCollectionName(String objectCatalogCollectionName) {
        this.objectCatalogCollectionName = objectCatalogCollectionName;
    }

    /**
     * Gets the list of server names
     * 
     * @return list of server names
     */
    public String getServers() {
        return servers;
    }

    /**
     * Set the list of server names to use
     * 
     * @param servers The list of server names
     */
    public void setServers(String servers) {
        this.servers = servers;
    }

    /**
     * Get the port to connect on
     * 
     * @return The port
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the port to connect on
     * 
     * @param port The port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets how many concurrent connections per host
     * 
     * @return number of connections per host
     */
    public int getConnPerHost() {
        return connPerHost;
    }

    /**
     * Gets how many concurrent connections per host
     * 
     * @param connPerHost number of connections per host
     */
    public void setConnPerHost(int connPerHost) {
        this.connPerHost = connPerHost;
    }

    /**
     * Gets the multiplier used to calculate how many blocking threads there are
     * 
     * @return The multiplier
     */
    public int getThreadBlockMultiplier() {
        return threadBlockMultiplier;
    }

    /**
     * Sets the multiplier used to calculate how many blocking threads there are
     * 
     * @param threadBlockMultiplier The multiplier
     */
    public void setThreadBlockMultiplier(int threadBlockMultiplier) {
        this.threadBlockMultiplier = threadBlockMultiplier;
    }

    /**
     * Gets the username used to connect. Must set useAuth to true
     * 
     * @return the username
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the username used to connect. Must set useAuth to true
     * 
     * @param userName the username
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Gets the password used to connect.
     * 
     * @return The password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Gets the password used to connect.
     * 
     * @param password The password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Get the default read preference used
     * 
     * @return the default read preference
     */
    public String getReadPreference() {
        return readPreference;
    }

    /**
     * Set the default read preference used
     * 
     * @param readPreference the default read preference
     */
    public void setReadPreference(String readPreference) {
        this.readPreference = readPreference;
        this.mongoReadPreference = decodeReadPreference(readPreference);
    }
    
    /**
     * Gets the Mongo ReadPreference object
     * 
     * @return The default MongoDB Read Preference
     */
    public ReadPreference getMongoReadPreference(){
        return mongoReadPreference;
    }
    
    /**
     * Sets the default write concern
     * 
     * @param writeConcern The write concern
     */
    public void setWriteConcern(String writeConcern){
        this.writeConcern = writeConcern;
        this.mongoWriteConcern = decodeWriteConcern(writeConcern);
    }
    
    /**
     * Gets the default write concern
     * 
     * @return The write concern
     */
    public WriteConcern getMongoWriteConcern(){
        return mongoWriteConcern;
    }
    
    /**
     * Looks up the read preference based on the string name.
     * <p>
     * secondaryPreferred is the default if it cannot find it
     * 
     * @param readPreference The read preference string to lookup
     * @return The Mongo ReadPreference object
     */
    private ReadPreference decodeReadPreference(String readPreference){
        ReadPreference rtn;
        switch (readPreference){
            case "PRIMARY":
                rtn = ReadPreference.primary();
                break;
            case "PREFER_PRIMARY":
                rtn = ReadPreference.primaryPreferred();
                break;
            case "SECONDARY":
                rtn = ReadPreference.secondary();
                break;
            case "PREFER_SECONDARY":
                rtn = ReadPreference.secondaryPreferred();
                break;
            case "NEAREST":
                rtn = ReadPreference.nearest();
                break;
            default:
                rtn = ReadPreference.secondaryPreferred();
        }
        return rtn;
    }
    
    /**
     * Looks up the Write Concern based on the string name.
     * <p>
     * MAJORITY is the default if it cannot find it
     * 
     * @param concern The write concern string to lookup
     * @return The Mongo ReadPreference object
     */
    private WriteConcern decodeWriteConcern(String concern){
        WriteConcern rtn;
        switch(concern){
            case "ACKNOWLEDGED":
                rtn = WriteConcern.ACKNOWLEDGED;
                break;
            case "MAJORITY":
                rtn = WriteConcern.MAJORITY;
                break;
            case "UNACKNOWLEDGED":
                rtn = WriteConcern.UNACKNOWLEDGED;
                break;
            default:
                rtn = WriteConcern.MAJORITY;
                break;
        }
        return rtn;
    }
}
