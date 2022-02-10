package org.datakow.catalogs.metadata.indexes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DatakowObjectMapper;
import java.util.Objects;

/**
 * Represents an individual field index definition.
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MongoIndexField {
    
    private String key;
    private String direction = "ASC";
    private String type = "DEFAULT";
    
    /**
     * Creates an index field definition with no parameters set.
     */
    public MongoIndexField(){}
    
    /**
     * Creates a default type index field on the key with the given direction
     * @param key The name of the property to be indexed
     * @param direction The direction ASC or DESC
     */
    public MongoIndexField(String key, String direction){
        this.key = key;
        this.direction = direction;
    }
    
    /**
     * Gets the name of the property that the index is for
     * 
     * @return the name of the property
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets the name of the property that the index is for.
     * 
     * @param key the name of the property
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Gets the direction that the index is oriented.
     * <p>
     * Possible values are ASC and DESC
     * <p>
     * Has no affect on geo indexes.
     * 
     * @return the direction that the index is oriented
     */
    public String getDirection() {
        return direction;
    }

    /**
     * Sets the direction that the index is oriented.
     * <p>
     * Possible values are ASC and DESC
     * <p>
     * Has no affect on geo indexes.
     * 
     * @param direction the direction that the index is oriented
     */
    public void setDirection(String direction) {
        this.direction = direction;
    }

    /**
     * Gets the type of index.
     * <p>
     * Possible values are DEFAULT, GEO, TEXT, 
     * 
     * @return the type of index
     */
    public String getType() {
        return type.toUpperCase();
    }

    /**
     * Sets the type of index.
     * <p>
     * Possible values are DEFAULT, GEO, TEXT, 
     * 
     * @param type the type of index
     */
    public void setType(String type) {
        if (type.equalsIgnoreCase("GEO") || type.equalsIgnoreCase("TEXT")){
            this.direction = null;
        }else{
            if (this.direction == null && this.direction.isEmpty()){
                this.direction = "ASC";
            }
        }
        this.type = type;
    }

    /**
     * Whether the index is a geometric index.
     * 
     * @return true if it is geometric
     */
    @JsonProperty("geo")
    public boolean isGeo() {
        return this.type.equalsIgnoreCase("GEO");
    }
    
    /**
     * Sets the type of this index to GEO or to DEFAULT if it was not already TEXT.
     * 
     * @param isGeo Whether the index is geometric or not
     */
    @JsonProperty("geo")
    public void setIsGeo(boolean isGeo) {
        if (isGeo){
            this.type = "GEO";
            this.direction = null;
        }else{
            if (!type.equalsIgnoreCase("TEXT")){
                this.type = "DEFAULT";
                if (this.direction == null || this.direction.isEmpty()){
                    this.direction = "ASC";
                }
            }
        }
    }

    /**
     * Returns true if the index is a TEXT index
     * 
     * @return true if the index is a TEXT index 
     */
    @JsonProperty("text")
    public boolean isText() {
        return this.type.equalsIgnoreCase("TEXT");
    }
    
    /**
     * Sets the type of index to TEXT or to DEFAULT if it was not already GEO.
     * 
     * @param isText true if the index should be a TEXT index
     */
    @JsonProperty("text")
    public void setIsText(boolean isText) {
        if (isText){
            this.type = "TEXT";
            this.direction = null;
        }else{
            if (!type.equalsIgnoreCase("GEO")){
                this.type = "DEFAULT";
                if (this.direction == null || this.direction.isEmpty()){
                    this.direction = "ASC";
                }
            }
        }
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static MongoIndexField fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        MongoIndexField record = mapper.readValue(json, MongoIndexField.class);
        return record;
    }
    
    @Override
    public boolean equals(Object obj){
        if (obj == null) {
            return false;
        }
        if (!MongoIndexField.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        if (obj == this){
            return true;
        }
        MongoIndexField other = (MongoIndexField)obj;
        
        boolean equals = (this.getKey() == null ? other.getKey() == null : this.getKey().equals(other.getKey()));
        equals = equals && (this.getDirection() == null ? other.getDirection() == null : this.getDirection().equals(other.getDirection()));
        equals = equals && (this.getType()== null ? other.getType() == null : this.getType().equals(other.getType()));
        
        return equals;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.key);
        hash = 97 * hash + Objects.hashCode(this.direction);
        hash = 97 * hash + Objects.hashCode(this.type);
        return hash;
    }
    
}
