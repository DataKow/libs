package org.datakow.messaging.events.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * An event that represents an action taken on a catalog.
 * <p>
 * This may be a catalog creation, modified index, updated catalog property, etc.
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogEvent extends Event{
    
    private String catalogIdentifier;
    
    /**
     * Creates a new catalog event with a unique ID of EventType.CATALOG
     */
    public CatalogEvent(){
        this.eventId = UUID.randomUUID().toString();
        this.eventType = EventType.CATALOG;
    }
    
    /**
     * Creates a new catalog event for a given event action.
     * <p>
     * The actions may be created, updated, deleted
     * 
     * @param eventAction The {@link EventAction}
     */
    public CatalogEvent(String eventAction){
        this.eventId = UUID.randomUUID().toString();
        this.eventType = EventType.CATALOG;
        this.eventAction = eventAction;
    }
    
    /**
     * Gets the catalog identifier that the event is for.
     * 
     * @return The catalog identifier
     */
    @JsonProperty("Catalog-Identifier")
    public String getCatalogIdentifier(){
        return catalogIdentifier;
    }
    
    /**
     * Sets the catalog identifier that the event is for
     * 
     * @param catalogIdentifier The catalog identifier
     */
    @JsonProperty("Catalog-Identifier")
    public void setCatalogIdentifier(String catalogIdentifier){
        this.catalogIdentifier = catalogIdentifier;
    }
    
}
