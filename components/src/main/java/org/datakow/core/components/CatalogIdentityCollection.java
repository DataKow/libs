package org.datakow.core.components;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A collection of {@link CatalogIdentity} objects. 
 * <p>
 * This class extends ArrayList and it adds a couple extra features.
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogIdentityCollection extends ArrayList<CatalogIdentity> implements JsonProducer{
    
    /**
     * Creates a new instance
     */
    public CatalogIdentityCollection(){
        
    }
    
    /**
     * Creates a new instance with an array of catalog identities
     * 
     * @param identities The catalog identities to initialize it with
     */
    public CatalogIdentityCollection(CatalogIdentity ... identities){
        this.addAll(Arrays.asList(identities));
    }
    
    /**
     * Gets a record from the collection if one exists.
     * 
     * @param catalogIdentifier The catalog identifier
     * @param recordIdentifier The record identifier
     * @return The found record or null
     */
    public CatalogIdentity get(String catalogIdentifier, String recordIdentifier){
        for(CatalogIdentity identity : this){
            if (identity.getCatalogIdentifier().equalsIgnoreCase(catalogIdentifier) &&
                identity.getRecordIdentifier().equalsIgnoreCase(recordIdentifier)){
                return identity;
            }
        }
        return null;
    }
    
    /**
     * Gets all catalog identities by the catalog identifier
     * 
     * @param catalogIdentifier The catalog identifier to search by
     * @return The discovered identities
     */
    public List<CatalogIdentity>get(String catalogIdentifier){
        List<CatalogIdentity> identities = new ArrayList<>();
        for(CatalogIdentity identity : this){
            if (identity.getCatalogIdentifier().equalsIgnoreCase(catalogIdentifier)){
                identities.add(identity);
            }
        }
        return identities;
    }
    
    /**
     * Checks if there is an identity in this collection with the catalog identifier
     * 
     * @param catalogIdentifier The catalog identifier to search for
     * @return true if the collection contains any
     */
    public boolean contains(String catalogIdentifier){
        return this.get(catalogIdentifier).size() > 0;
    }
    
    /**
     * Checks if there is an identity that matches
     * 
     * @param catalogIdentifier The catalog identifier
     * @param recordIdentifier The record identifier
     * @return true if it contains
     */
    public boolean contains(String catalogIdentifier, String recordIdentifier){
        return this.get(catalogIdentifier, recordIdentifier) != null;
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static CatalogIdentityCollection fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        CatalogIdentityCollection identity = mapper.readValue(json, CatalogIdentityCollection.class);
        return identity;
    }
    
    /**
     * Converts the catalog identities into the value for an Http Header
     * 
     * @return The http header string
     */
    public String toHttpHeader(){
        ArrayList<String> identities = new ArrayList<>();
        for(CatalogIdentity identity : this){
            identities.add(identity.getCatalogIdentifier() + ";" + identity.getRecordIdentifier());
        }
        return String.join(",", identities);
    }
    
    /**
     * Converts the HTTP header value into a collection of catalog identities
     * 
     * @param headerValue The String header value
     * @return The converted collection
     */
    public static CatalogIdentityCollection metadataAssociationFromHttpHeader(String headerValue){
        CatalogIdentityCollection coll = new CatalogIdentityCollection();
        String[] identityStrings = headerValue.split(",");
        for (String identityString : identityStrings) {
            CatalogIdentity identity = CatalogIdentity.fromHttpHeader(identityString.trim());
            if (identity != null){
                coll.add(identity);
            }
        }
        return coll;
    }
    

    
}
