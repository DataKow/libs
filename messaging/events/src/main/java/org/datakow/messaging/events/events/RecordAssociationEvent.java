package org.datakow.messaging.events.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.DatakowObjectMapper;

import java.util.Objects;
import java.util.UUID;

/**
 * And event that is caused when two records have been associated by either the
 * object or metadata catalog.
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordAssociationEvent extends Event {

    private CatalogIdentity objectIdentity;
    private CatalogIdentity objectMetadataIdentity;
    
    /**
     * Creates a new instance with a random UUID id and of event type catalog record association
     */
    public RecordAssociationEvent(){
        this.eventId = UUID.randomUUID().toString();
        this.eventType= EventType.CATALOG_RECORD_ASSOCIATION;
    }
    
    /**
     * Creates a new instance for a given event action
     * 
     * @param eventAction The action
     */
    public RecordAssociationEvent(String eventAction){
        this.eventId = UUID.randomUUID().toString();
        this.eventAction = eventAction;
        this.eventType = EventType.CATALOG_RECORD_ASSOCIATION;
    }
    
    /**
     * Creates a new instance with all of the properties needed to send.
     * 
     * @param eventAction The event action
     * @param objectIdentity The object catalog identity
     * @param objectMetadataIdentity The metadata catalog identity
     */
    public RecordAssociationEvent(String eventAction, CatalogIdentity objectIdentity, CatalogIdentity objectMetadataIdentity){
        this.eventId = UUID.randomUUID().toString();
        this.eventAction = eventAction;
        this.eventType = EventType.CATALOG_RECORD_ASSOCIATION;
        this.objectIdentity = objectIdentity;
        this.objectMetadataIdentity = objectMetadataIdentity;
    }
    
    /**
     * Gets the Identity of the object record that was associated
     * 
     * @return The object catalog identity
     */
    @JsonProperty("Object-Identity")
    public CatalogIdentity getObjectIdentity(){
        return this.objectIdentity;
    }
    
    /**
     * Sets the Identity of the object record that was associated
     * 
     * @param identity The object catalog identity
     */
    @JsonProperty("Object-Identity")
    public void setObjectIdentity(CatalogIdentity identity){        
        this.objectIdentity = identity;
    }
    
    /**
     * Gets the identity of the metadata catalog record that was associated
     * 
     * @return the metadata identity
     */
    @JsonProperty("Object-Metadata-Identity")
    public CatalogIdentity getObjectMetadataIdentity(){
        return this.objectMetadataIdentity;
    }
    
    /**
     * Sets the identity of the metadata catalog record that was associated
     * 
     * @param identity the metadata identity
     */
    @JsonProperty("Object-Metadata-Identity")
    public void setObjectMetadataIdentity(CatalogIdentity identity){
        this.objectMetadataIdentity = identity;
    }
    
    @Override
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static RecordAssociationEvent fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        RecordAssociationEvent identity = mapper.readValue(json, RecordAssociationEvent.class);
        return identity;
    }
    
    @Override
    public boolean equals(Object obj){
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RecordAssociationEvent other = (RecordAssociationEvent)obj;

        if (!this.getObjectIdentity().equals(other.getObjectIdentity())){
            return false;
        }
        if (!this.getObjectMetadataIdentity().equals(other.getObjectMetadataIdentity())){
            return false;
        }
        boolean same = this.getEventId() == null ? other.getEventId() == null : this.getEventId().equalsIgnoreCase(other.getEventId());

        return same;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Objects.hashCode(this.eventId);
        hash = 89 * hash + Objects.hashCode(this.objectIdentity);
        hash = 89 * hash + Objects.hashCode(this.objectMetadataIdentity);
        return hash;
    }
}
