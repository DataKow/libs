package org.datakow.core.components;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that represents the identity of a record in a catalog
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogIdentity implements JsonProducer{
    
    private String catalogIdentifier;
    private String recordIdentifier;

    //private static Pattern pattern = Pattern.compile("catalogs/(.+)/records/([a-zA-Z0-9\\-]+)");
    private static Pattern pattern = Pattern.compile("catalogs/(.+)/(records|objects)/([a-zA-Z0-9\\-]+)");
    
    /**
     * Creates a new instance
     */
    public CatalogIdentity(){}
    
    /**
     * Creates a new instance with the full identity
     * 
     * @param catalogIdentifier The identifier of the catalog
     * @param recordIdentifier The ID of the record
     */
    public CatalogIdentity(String catalogIdentifier, String recordIdentifier){
        this.catalogIdentifier = catalogIdentifier;
        this.recordIdentifier = recordIdentifier;
    }
    
    /**
     * Gets the catalog identifier
     * 
     * @return The catalog identifier
     */
    @JsonProperty("Catalog-Identifier")
    public String getCatalogIdentifier() {
        return catalogIdentifier;
    }

    /**
     * Sets the catalog identifier
     * 
     * @param CatalogIdentifier The catalog identifier
     */
    @JsonProperty("Catalog-Identifier")
    public void setCatalogIdentifier(String CatalogIdentifier) {
        this.catalogIdentifier = CatalogIdentifier;
    }

    /**
     * Gets the record identifier
     * 
     * @return the record id 
     */
    @JsonProperty("Record-Identifier")
    public String getRecordIdentifier() {
        return recordIdentifier;
    }

    /**
     * Gets the record identifier
     * 
     * @param RecordIdentifier the record id 
     */
    @JsonProperty("Record-Identifier")
    public void setRecordIdentifier(String RecordIdentifier) {
        this.recordIdentifier = RecordIdentifier;
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static CatalogIdentity fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        CatalogIdentity identity = mapper.readValue(json, CatalogIdentity.class);
        return identity;
    }
            
    public static CatalogIdentity fromHttpHeader(String identityString){
        CatalogIdentity identity = null;
        if (identityString != null && !identityString.isEmpty()){
            String[] catalog_obj = identityString.split(";");
            if (catalog_obj != null && catalog_obj.length == 2){
                identity = new CatalogIdentity(catalog_obj[0].trim(), catalog_obj[1].trim());
            }
        }
        return identity;
    }
    
    public static CatalogIdentity fromUrl(String url){
        CatalogIdentity identity = null;
        if (url != null && !url.isEmpty()){
            Matcher matcher = pattern.matcher(url);
            if (matcher.find()){
                String catalogIdentifier = matcher.group(1);
                String recordIdentifier = matcher.group(3);
                identity = new CatalogIdentity(catalogIdentifier, recordIdentifier);
            }
        }
        return identity;
    }
    
    public String toRelativeMetadataUrl(){
        return "/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier;
    }
    
    public String toRelativeObjectUrl(){
        return "/catalogs/" + catalogIdentifier + "/objects/" + recordIdentifier;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.catalogIdentifier);
        hash = 37 * hash + Objects.hashCode(this.recordIdentifier);
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
        final CatalogIdentity other = (CatalogIdentity) obj;
        if (!Objects.equals(this.catalogIdentifier, other.catalogIdentifier)) {
            return false;
        }
        if (!Objects.equals(this.recordIdentifier, other.recordIdentifier)) {
            return false;
        }
        return true;
    }
    
    
}
