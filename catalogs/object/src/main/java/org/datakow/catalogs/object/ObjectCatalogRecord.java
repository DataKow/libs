package org.datakow.catalogs.object;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DatakowObjectMapper;
import java.util.Date;
import javax.xml.bind.DatatypeConverter;

/**
 * Represents a record that is stored in the Object Catalog
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectCatalogRecord extends ObjectCatalogRecordInput {
    
    protected String id;
    protected Date publishDate;
    protected String contentMD5;
    
    /**
     * Sets the record's ID
     * 
     * @param id The id
     */
    @JsonProperty("Record-Identifier")
    public void setId(String id){
        this.id = id;
    }
    
    /**
     * Gets the record's ID
     * 
     * @return The id
     */
    @JsonProperty("Record-Identifier")
    public String getId(){
        return this.id;
    }
    
    /**
     * Gets the record's publish date
     * 
     * @return the publish date
     */
    @JsonProperty("Publish-Date")
    public Date getPublishDate() {
        return publishDate;
    }

    /**
     * Sets the record's publish date
     * 
     * @param publishDate the publish date
     */
    @JsonProperty("Publish-Date")
    public void setPublishDate(Date publishDate) {
        this.publishDate = publishDate;
    }
    
    /**
     * Gets the record's MD5 hash
     * 
     * @return the MD5 hash
     */
    @JsonProperty("Content-MD5")
    public String getContentMD5(){
        return this.contentMD5;
    }
    
    /**
     * Sets the record's MD5 hash
     * 
     * @param md5 the MD5 hash
     */
    @JsonProperty("Content-MD5")
    public void setContentMD5(String md5){
        this.contentMD5 = md5;
    }
    
    
    @Override
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static ObjectCatalogRecord fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        ObjectCatalogRecord identity = mapper.readValue(json, ObjectCatalogRecord.class);
        return identity;
    }
    
    public static ObjectCatalogRecord fromBase64EncodedJson(String base64Encoded) throws JsonProcessingException{
        String parsed = new String(DatatypeConverter.parseBase64Binary(base64Encoded));
        ObjectCatalogRecord out = ObjectCatalogRecord.fromJson(parsed);
        return out;
    }
    
    @Override
    public String toBase64EncodedJson() throws JsonProcessingException{
        return DatatypeConverter.printBase64Binary(toJson().getBytes());
    }
    
}
