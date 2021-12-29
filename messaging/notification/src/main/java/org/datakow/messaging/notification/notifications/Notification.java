package org.datakow.messaging.notification.notifications;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.DatakowObjectMapper;
import java.io.IOException;

import java.util.Objects;
import java.util.UUID;

/**
 * The representation of a notification sent and received in the DATAKOW notification system.
 * 
 * @author kevin.off
 */
public class Notification {
    
    protected String notificationId;
    protected CatalogIdentity objectIdentity;
    protected CatalogIdentity objectMetadataIdentity;

    /**
     * Creates a new instance with a unique id
     */
    public Notification(){
        this.notificationId = UUID.randomUUID().toString();
    }

    /**
     * Gets the ID of the notification
     * 
     * @return The notification Id
     */
    @JsonProperty("Notification-Identifier")
    public String getNotificationId(){
        return this.notificationId;
    }
    
    /**
     * Sets the ID of the notification
     * 
     * @param id The notification Id
     */
    @JsonProperty("Notification-Identifier")
    public void setNotificationId(String id){
        this.notificationId = id;
    }
    
    /**
     * Gets the identity of the object for the notification
     * 
     * @return The identity of the object
     */
    @JsonProperty("Object-Identity")
    public CatalogIdentity getObjectIdentity(){
        return this.objectIdentity;
    }
    
    /**
     * Sets the identity of the object for the notification
     * 
     * @param identity The identity of the object
     */
    @JsonProperty("Object-Identity")
    public void setObjectIdentity(CatalogIdentity identity){
        this.objectIdentity = identity;
    }
    
    /**
     * Gets the identity of the metadata record for the notification
     * 
     * @return The identity of the object metadata record
     */
    @JsonProperty("Metadata-Identity")
    public CatalogIdentity getObjectMetadataIdentity(){
        return this.objectMetadataIdentity;
    }
    
    /**
     * Sets the identity of the metadata record for the notification
     * 
     * @param identity The identity of the metadata record
     */
    @JsonProperty("Metadata-Identity")
    public void setObjectMetadataIdentity(CatalogIdentity identity){
        this.objectMetadataIdentity = identity;
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static Notification fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        Notification identity = mapper.readValue(json, Notification.class);
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
        Notification other = (Notification)obj;
        if (!this.getObjectIdentity().equals(other.getObjectIdentity())){
            return false;
        }
        if (!this.getObjectMetadataIdentity().equals(other.getObjectMetadataIdentity())){
            return false;
        }
        boolean same = this.getNotificationId() == null ? other.getNotificationId() == null : this.getNotificationId().equalsIgnoreCase(other.getNotificationId());

        return same;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Objects.hashCode(this.notificationId);
        hash = 89 * hash + Objects.hashCode(this.objectIdentity);
        hash = 89 * hash + Objects.hashCode(this.objectMetadataIdentity);
        return hash;
    }
}
