package org.datakow.messaging.events.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.DatakowObjectMapper;

import java.util.UUID;

/**
 * An event caused when a record in a catalog gets created, updated, or deleted
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordEvent extends Event{
 
    protected CatalogIdentity identity;

    /**
     * Do not use this constructor. It is intended to be used by Jackson2 
     * JSON deserializer only. If you must, make sure to set the eventAction.
     */
    public RecordEvent(){
        this.eventId = UUID.randomUUID().toString();
        this.eventType = EventType.CATALOG_RECORD;
    }
    
    /**
     * Creates an instance for the given event action
     * 
     * @param eventAction The event action
     */
    public RecordEvent(String eventAction){
        this.eventId = UUID.randomUUID().toString();
        this.eventAction = eventAction;
        this.eventType = EventType.CATALOG_RECORD;
    }
    
    /**
     * Creates an instance with the event action and the catalog identity of
     * the record that caused the event.
     * 
     * @param eventAction The event action
     * @param identity The identity of the record
     */
    public RecordEvent(String eventAction, CatalogIdentity identity){
        this.eventId = UUID.randomUUID().toString();
        this.eventAction = eventAction;
        this.eventType = EventType.CATALOG_RECORD;
        this.identity = identity;
    }
    
    /**
     * Gets the identity of the record that caused the event
     * 
     * @return The identity of the record
     */
    @JsonProperty("Catalog-Identity")
    public CatalogIdentity getCatalogIdentity(){
        return identity;
    }
    
    /**
     * Sets the identity of the record that caused the event
     * 
     * @param identity The identity of the record
     */
    @JsonProperty("Catalog-Identity")
    public void setCatalogIdentity(CatalogIdentity identity){
        this.identity = identity;
    }
    
    @Override
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static RecordEvent fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        RecordEvent identity = mapper.readValue(json, RecordEvent.class);
        return identity;
    }
    
}
