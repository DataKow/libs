package org.datakow.catalogs.object;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DatakowObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.DatatypeConverter;
//import org.apache.commons.io.IOUtils;


/**
 * Represents the base set of properties needed to create an object in the catalog.
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectCatalogRecordInput {
    
    protected String contentType;
    protected String contentEncoding;
    protected long contentLength;
    protected CatalogIdentityCollection objectMetadataIdentities = new CatalogIdentityCollection();
    protected List<String> tags = new ArrayList<>();
    protected InputStream objectData;
    protected String publisher;
    protected String realm;
    protected List<String> metadataCatalogIdentifiers;
    
    
    /**
     * Gets the object's content type. 
     * (mime type)
     * 
     * @return the objects mime type
     */
    @JsonProperty("Content-Type")
    public String getContentType() {
        return contentType;
    }

    /**
     * Sets the object's content type. 
     * (mime type)
     * 
     * @param contentType the objects mime type
     */
    @JsonProperty("Content-Type")
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    /**
     * Gets the object's content encoding
     * 
     * @return The content encoding
     */
    @JsonProperty("Content-Encoding")
    public String getContentEncoding(){
        return this.contentEncoding;
    }
    
    /**
     * Sets the object's content encoding
     * 
     * @param contentEncoding The content encoding
     */
    @JsonProperty("Content-Encoding")
    public void setContentEncoding(String contentEncoding){
        this.contentEncoding = contentEncoding;
    }
    
    /**
     * Gets the object's data's content length in bytes
     * 
     * @return the content length of the data in bytes
     */
    @JsonProperty("Content-Length")
    public long getContentLength() {
        return contentLength;
    }

    /**
     * Sets the object's data's content length in bytes
     * 
     * @param contentLength the content length of the data in bytes
     */
    @JsonProperty("Content-Length")
    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }
    
    /**
     * Gets the username of the objects publisher
     * 
     * @return The publisher's username
     */
    @JsonProperty("Publisher")
    public String getPublisher(){
        return this.publisher;
    }
    
    /**
     * Sets the username of the objects publisher
     * 
     * @param publisher The publisher's username
     */
    @JsonProperty("Publisher")
    public void setPublisher(String publisher){
        this.publisher = publisher;
    }

    /**
     * Gets the identities of any associated records
     * 
     * @return the identities of any associated records
     */
    @JsonProperty("Metadata-Identities")
    public CatalogIdentityCollection getObjectMetadataIdentities() {
        return objectMetadataIdentities;
    }

    /**
     * Sets the identities of any associated records
     * 
     * @param objectMetadataIdentities the identities of any associated records
     */
    @JsonProperty("Metadata-Identities")
    public void setObjectMetadataIdentities(CatalogIdentityCollection objectMetadataIdentities) {
        this.objectMetadataIdentities = objectMetadataIdentities;
    }

    public void addObjectMetadataIdentity(CatalogIdentity identity){
        this.objectMetadataIdentities.add(identity);
    }

    // @JsonProperty("Metadata-Catalog-Identifiers")
    // public List<String> getMetadataCatalogIdentifiers() {
    //     return metadataCatalogIdentifiers;
    // }

    // @JsonProperty("Metadata-Catalog-Identifiers")
    // public void setMetadataCatalogIdentifiers(String ... metadataCatalogIdentifiers) {
    //     this.metadataCatalogIdentifiers = Arrays.asList(metadataCatalogIdentifiers);
    // }

    // @JsonProperty("Metadata-Catalog-Identifiers")
    // public void setMetadataCatalogIdentifiers(List<String> metadataCatalogIdentifiers) {
    //     this.metadataCatalogIdentifiers = metadataCatalogIdentifiers;
    // }
    
    /**
     * Sets the tags for an object
     * 
     * @param tags the tags
     */
    @JsonProperty("Tags")
    public void setTags(List<String> tags){
        this.tags = tags;
    }
    
    /**
     * Gets the tags for an object
     * 
     * @return the tags
     */
    @JsonProperty("Tags")
    public List<String> getTags(){
        return this.tags;
    }
    
    /**
     * Sets the security realm of the object
     * 
     * @param realm the security realm name
     */
    @JsonProperty("Realm")
    public void setRealm(String realm){
        this.realm = realm;
    }
    
    /**
     * gets the security realm of the object
     * 
     * @return the security realm name
     */
    @JsonProperty("Realm")
    public String getRealm(){
        return this.realm;
    }
    
    /**
     * Gets the data's input stream to read from
     * 
     * @return the data's input stream
     */
    @JsonIgnore
    public InputStream getData(){
        return this.objectData;
    }
    
    /**
     * Sets the data's input stream to read from
     * 
     * @param data the data's input stream
     */
    @JsonIgnore
    public void setData(InputStream data){
        this.objectData = data;
    }

    public void setData(String data){
        this.setData(new ByteArrayInputStream(data.getBytes()));
    }
    
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static ObjectCatalogRecordInput fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        ObjectCatalogRecordInput identity = mapper.readValue(json, ObjectCatalogRecordInput.class);
        return identity;
    }
    
    public static ObjectCatalogRecordInput fromBase64EncodedJson(String base64Encoded) throws JsonProcessingException{
        String parsed = new String(DatatypeConverter.parseBase64Binary(base64Encoded));
        ObjectCatalogRecordInput out = ObjectCatalogRecordInput.fromJson(parsed);
        return out;
    }
    
    /**
     * Gets a string representation of the Base64Encoded JSON of the object without the data
     * @return the encoded string
     * @throws JsonProcessingException If there is an error processing the object to JSON
     */
    public String toBase64EncodedJson() throws JsonProcessingException{
        return DatatypeConverter.printBase64Binary(toJson().getBytes());
    }
    
}
