package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.JsonProducer;
import org.datakow.core.components.DatakowObjectMapper;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The Storage portion of a {@link MetadataCatalogRecord} object
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataCatalogRecordStorage implements JsonProducer{
    
    private String realm;
    private String recordIdentifier;
    private String publisher;
    private Date publishDate;
    private List<String> tags;
    private Date updateDate;
    private String updatedBy;
    private CatalogIdentityCollection objectIdentities;
    

    /**
     * Gets the Record Identifier of this record.
     * 
     * @return The record identifier
     */
    @JsonProperty("Record-Identifier")
    public String getId() {
        return recordIdentifier;
    }

    /**
     * Sets the Record Identifier of this record.
     * 
     * @param recordIdentifier The record identifier
     */
    @JsonProperty("Record-Identifier")
    public void setId(String recordIdentifier) {
        this.recordIdentifier = recordIdentifier;
    }
    
    /**
     * Gets the collection of associated record's identities
     * 
     * @return The collection of associated records identities
     */
    @JsonProperty("Object-Identities")
    public CatalogIdentityCollection getObjectIdentities(){
        return this.objectIdentities;
    }
    
    /**
     * Sets the collection of associated record's identities
     * 
     * @param identities The collection of associated records identities
     */
    @JsonProperty("Object-Identities")
    public void setObjectIdentities(CatalogIdentityCollection identities){
        this.objectIdentities = identities;
    }
    
    /**
     * Gets the security realm for this record.
     * 
     * @return The security realm
     */
    @JsonProperty("Realm")
    public String getRealm() {
        return realm;
    }

    /**
     * Sets the security realm for this record.
     * 
     * @param realm The security realm
     */
    @JsonProperty("Realm")
    public void setRealm(String realm) {
        this.realm = realm;
    }

    /**
     * Gets the username of the publisher for this record.
     * 
     * @return The publisher's name
     */
    @JsonProperty("Publisher")
    public String getPublisher() {
        return publisher;
    }

    /**
     * Sets the username of the publisher for this record.
     * 
     * @param publisher The publisher's name
     */
    @JsonProperty("Publisher")
    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    /**
     * Gets the data that this record was published.
     * 
     * @return The date of publish
     */
    @JsonProperty("Publish-Date")
    public Date getPublishDate() {
        return publishDate;
    }

    /**
     * Sets the data that this record was published.
     * 
     * @param publishDate The date of publish
     */
    @JsonProperty("Publish-Date")
    public void setPublishDate(Date publishDate) {
        this.publishDate = publishDate;
    }

    /**
     * Gets the list of tags for this record.
     * 
     * @return The list of tags
     */
    @JsonProperty("Tags")
    public List<String> getTags() {
        return tags;
    }

    /**
     * Sets the list of tags for this record.
     * 
     * @param tags The list of tags
     */
    @JsonProperty("Tags")
    public void setTags(List<String> tags) {
        this.tags = tags;
    }
    
    /**
     * Gets the date that this record was modified.
     * 
     * @return The modified date
     */
    @JsonProperty("Update-Date")
    public Date getUpdateDate() {
        return updateDate;
    }
    
    /**
     * Sets the date that this record was modified.
     * 
     * @param updateDate The modified date
     */
    @JsonProperty("Update-Date")
    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }
    
    /**
     * Similar to Publisher, gets the username of the person who modified the record.
     * 
     * @return The username who last modified the record
     */
    @JsonProperty("Updated-By")
    public String getUpdatedBy() {
        return updatedBy;
    }
    
    /**
     * Similar to Publisher, Sets the username of the person who modified the record.
     * 
     * @param updatedBy The username who last modified the record
     */
    @JsonProperty("Updated-By")
    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static MetadataCatalogRecordStorage fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        MetadataCatalogRecordStorage record = mapper.readValue(json, MetadataCatalogRecordStorage.class);
        return record;
    }
    
    public static MetadataCatalogRecordStorage fromMap(Map map) throws IllegalArgumentException{
        return DatakowObjectMapper.getObjectMapper().convertValue(map, MetadataCatalogRecordStorage.class);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 73 * hash + Objects.hashCode(this.realm);
        hash = 73 * hash + Objects.hashCode(this.recordIdentifier);
        hash = 73 * hash + Objects.hashCode(this.publisher);
        hash = 73 * hash + Objects.hashCode(this.publishDate);
        hash = 73 * hash + Objects.hashCode(this.tags);
        hash = 73 * hash + Objects.hashCode(this.updateDate);
        hash = 73 * hash + Objects.hashCode(this.updatedBy);
        hash = 73 * hash + Objects.hashCode(this.objectIdentities);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MetadataCatalogRecordStorage other = (MetadataCatalogRecordStorage) obj;
        if (!Objects.equals(this.realm, other.realm)) {
            return false;
        }
        if (!Objects.equals(this.recordIdentifier, other.recordIdentifier)) {
            return false;
        }
        if (!Objects.equals(this.publisher, other.publisher)) {
            return false;
        }
        if (!Objects.equals(this.updatedBy, other.updatedBy)) {
            return false;
        }
        if (!Objects.equals(this.publishDate, other.publishDate)) {
            return false;
        }
        if (!Objects.equals(this.tags, other.tags)) {
            return false;
        }
        if (!Objects.equals(this.updateDate, other.updateDate)) {
            return false;
        }
        if (!Objects.equals(this.objectIdentities, other.objectIdentities)) {
            return false;
        }
        return true;
    }
    
    
    
}
