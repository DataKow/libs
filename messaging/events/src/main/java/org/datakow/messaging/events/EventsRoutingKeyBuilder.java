package org.datakow.messaging.events;

/**
 * The routing key builder used to build routing keys for the DATAKOW messaging system.
 * 
 * @author kevin.off
 */
public class EventsRoutingKeyBuilder {
    
    private final String messagingVersion;
    private final String environment;
    private final String applicationName;
    
    /**
     * Creates a new instance of the builder with the version, environment, and application name.
     * These properties are all used in the routing keys
     * 
     * @param messagingVersion The version of the DATAKOW messaging system
     * @param environment The environment that the system is running in
     * @param applicationName The application name that is accessing the messaging system
     */
    public EventsRoutingKeyBuilder(String messagingVersion, String environment, String applicationName){
        this.messagingVersion = messagingVersion;
        this.environment = environment;
        this.applicationName = applicationName;
    }
    
    /**
     * Builds a routing key used to send events.
     * 
     * @param serviceName The name of the service that is sending the event
     * @param eventType The {@link org.datakow.messaging.events.events.EventType} of the event
     * @param subjectName The name of the subject of the event
     * @param subjectDetail The detail element for this event (could be an id)
     * @param action The {@link org.datakow.messaging.events.events.EventAction}
     * @return The built routing key
     */
    public String buildRoutingKey(String serviceName, String eventType, String subjectName, String subjectDetail, String action){
        StringBuilder builder = new StringBuilder();
        builder.append("org").append(".") //organization group 
                .append("datakow").append(".") //organization name
                .append(messagingVersion).append(".") //messaging version
                .append(serviceName).append(".") //service name
                .append(environment).append(".") //environment
                .append(eventType).append(".") //event type
                .append(subjectName).append(".") //subject name
                .append(subjectDetail).append(".") //subject detail
                .append(action); //action taken
        return builder.toString().toLowerCase();
    }
    
    /**
     * Builds a routing key used to send events.
     * 
     * @param eventType The {@link org.datakow.messaging.events.events.EventType} of the event
     * @param subjectName The name of the subject of the event
     * @param subjectDetail The detail element for this event (could be an id)
     * @param action The {@link org.datakow.messaging.events.events.EventAction}
     * @return The built routing key
     */
    public String buildRoutingKey(String eventType, String subjectName, String subjectDetail, String action){
        return this.buildRoutingKey(applicationName, eventType, subjectName, subjectDetail, action);
    }
}
