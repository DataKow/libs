package org.datakow.messaging.notification;


/**
 * The routing key builder used to build routing keys for the DATAKOW messaging system.
 * 
 * @author kevin.off
 */
public class ServicesRoutingKeyBuilder {
    private final String version;
    private final String env;
    
    /**
     * Creates a new instance of the builder with the version, environment, and application name.
     * These properties are all used in the routing keys
     * 
     * @param messagingVersion The version of the DATAKOW messaging system
     * @param environment The environment that the system is running in
     */
    public ServicesRoutingKeyBuilder(String messagingVersion, String environment){
        this.version = messagingVersion;
        this.env = environment;
    }
    
    /**
     * Builds a routing key used to send events.
     * 
     * @param serviceName The name of the service that is sending the event
     * @param serviceGroup The group of the service
     * @param commandType The type of command that was being sent
     * @param id The ID of the message
     * @return The built routing key
     */
    public String buildRoutingKey(String serviceName, String serviceGroup, String commandType, String id){
        
        StringBuilder builder = new StringBuilder();
        builder.append("org").append(".") //organization group 
                .append("datakow").append(".") //organization name
                .append(version).append(".") //messaging version
                .append(serviceName).append(".") //service name
                .append(env).append(".") //environment
                .append(serviceGroup).append(".") //service group
                .append(commandType).append(".") //method name
                .append(id); //method detail
        return builder.toString().toLowerCase();
    }
}
