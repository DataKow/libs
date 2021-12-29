package org.datakow.messaging.events.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DatakowObjectMapper;

/**
 * The super class that all events must extend.
 * This class contains properties that all events have in common.
 * Jackson2Json also uses this class to determine what kind of event it is
 * receiving.
 * 
 * @author kevin.off
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "Event-Type")
@JsonSubTypes({
    @Type(value=RecordEvent.class, name=EventType.CATALOG_RECORD),
    @Type(value=RecordAssociationEvent.class, name=EventType.CATALOG_RECORD_ASSOCIATION),
    @Type(value=CatalogEvent.class, name=EventType.CATALOG),
    @Type(value=SubscriptionEvent.class, name=EventType.SUBSCRIPTION),
    @Type(value=DataFileEvent.class, name=EventType.DATA_FILE),
    @Type(value=ModelIngestFile.class, name =EventType.MODEL_INGEST_FILE)
})
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Event {

    protected String eventId;
    protected String eventType;
    protected String eventAction = "event";
    
    /**
     * Gets the identifier of the event
     * 
     * @return The event id
     */
    @JsonProperty("Event-Identifier")
    public String getEventId(){
        return this.eventId;
    }
    
    /**
     * Sets the identifier of the event
     * 
     * @param id The event id
     */
    @JsonProperty("Event-Identifier")
    public void setEventId(String id){
        this.eventId = id;
    }
    
    /**
     * Sets the event type of this event.
     * 
     * @param type The type of event
     */
    @JsonProperty("Event-Type")
    @JsonIgnore //Ignored because the JsonTypeInfo declaration takes care of it
    public void setEventType(String type){
        this.eventType = type;
    }

    /**
     * Gets the event type of this event.
     * 
     * @return The type of event
     */
    @JsonProperty("Event-Type")
    @JsonIgnore //Ignored because the JsonTypeInfo declaration takes care of it
    public String getEventType(){
        return this.eventType;
    }  
    
    /**
     * Sets the action that was taken to cause the event
     * 
     * @param eventAction The event action
     */
    @JsonProperty("Event-Action")
    public void setEventAction(String eventAction){
        this.eventAction = eventAction;
    }
    
    /**
     * Sets the action that was taken to cause the event
     * 
     * @return The event action
     */
    @JsonProperty("Event-Action")
    public String getEventAction(){
        return this.eventAction;
    }
    
    public static <T> T fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        T command = (T)mapper.readValue(json, Event.class);
        return command;
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    @Override
    public String toString(){
        try{
            return this.toJson();
        }catch(JsonProcessingException e){
            throw new RuntimeException("Error converting Event to a JSON string", e);
        }
    }
    
}
