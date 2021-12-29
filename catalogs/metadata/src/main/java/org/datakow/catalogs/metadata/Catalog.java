package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.catalogs.metadata.indexes.MongoIndex;
import org.datakow.catalogs.metadata.jsonschema.JsonSchema;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.DatakowObjectMapper;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that represents a catalog within the Metadata Catalog Web Service.
 * 
 * @author kevin.off
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Catalog {
    
    String collectionName;
    String catalogIdentifier;
    private List<MongoIndex> indexes = new ArrayList<>();
    private JsonSchema schema;
    private String catalogType;
    private List<DataRetentionPolicy> dataRetentionPolicy;
    private long numRecords;
    private long size;

    /**
     * Do not use this constructor.
     * It is intended for jackson json deserialization
     * If you must, make sure to set the catalog type.
     */
    public Catalog(){
        
    }
    
    /**
     * Creates an instance of a catalog object with a given catalog type.
     * <p>
     * The different types can be metadata, object, system
     * 
     * @param catalogType The type of catalog
     */
    public Catalog(String catalogType){
        this.catalogType = catalogType;
    }
    
    /**
     * Gets the backing MongoDB collection name for the catalog.
     * 
     * @return The catalog's collection name
     */
    @JsonProperty("Collection-Name")
    public String getCollectionName() {
        return collectionName;
    }
    
    /**
     * Sets the backing MongoDB collection name for the catalog.
     * 
     * @param collectionName The catalog's collection name
     */
    @JsonProperty("Collection-Name")
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Gets the virtual catalog identifier for this catalog.
     * <p>
     * This is the name that is used to identify the catalog in places
     * such as in the URL of a request to the Metadata Catalog Web Service.
     * 
     * @return The virtual catalog identifier
     */
    @JsonProperty("Catalog-Identifier")
    public String getCatalogIdentifier() {
        return catalogIdentifier;
    }

    /**
     * Sets the virtual catalog identifier for this catalog.
     * <p>
     * This is the name that is used to identify the catalog in places
     * such as in the URL of a request to the Metadata Catalog Web Service.
     * 
     * @param catalogIdentifier The virtual catalog identifier
     */
    @JsonProperty("Catalog-Identifier")
    public void setCatalogIdentifier(String catalogIdentifier) {
        this.catalogIdentifier = catalogIdentifier;
    }
    
    /**
     * Gets the type of catalog.
     * <p>
     * Can be metadata, object, or system
     * 
     * @return the catalog type
     */
    @JsonProperty("Catalog-Type")
    public String getCatalogType(){
        return this.catalogType;
    }

    /**
     * Sets the type of catalog.
     * <p>
     * Can be metadata, object, or system
     * 
     * @param type the catalog type
     */
    @JsonProperty("Catalog-Type")
    public void setCatalogType(String type){
        this.catalogType = type;
    }
    
    /**
     * Gets the list of indexes that are on the MongoDB collection
     * 
     * @return The list of indexes
     */
    @JsonProperty("Indexes")
    public List<MongoIndex> getIndexes() {
        return indexes;
    }
    
    /**
     * Sets the list of indexes that are on the MongoDB collection
     * 
     * @param indexes The list of indexes
     */
    @JsonProperty("Indexes")
    public void setIndexes(List<MongoIndex> indexes) {
        this.indexes = indexes;
    }

    /**
     * Gets the schema that is assigned to the catalog.
     * 
     * @return The schema
     */
    @JsonProperty("Schema")
    public JsonSchema getSchema() {
        return schema;
    }

    /**
     * Sets the schema that is assigned to the catalog.
     * 
     * @param schema The schema
     */
    @JsonProperty("Schema")
    public void setSchema(JsonSchema schema) {
        this.schema = schema;
    }

    /**
     * Gets the list of retention policy objects that are assigned to the catalog
     * 
     * @return The list of retention policy objects
     */
    @JsonProperty("Retention-Policy")
    public List<DataRetentionPolicy> getDataRetentionPolicy() {
        return dataRetentionPolicy;
    }

    /**
     * Sets the list of retention policy objects that are assigned to the catalog
     * 
     * @param dataRetentionPolicy The list of retention policy objects
     */
    @JsonProperty("Retention-Policy")
    public void setDataRetentionPolicy(List<DataRetentionPolicy> dataRetentionPolicy) {
        this.dataRetentionPolicy = dataRetentionPolicy;
    }

    /**
     * Gets the number of records that are in the catalog.
     * 
     * @return The number of records
     */
    @JsonProperty("Num-Records")
    public long getNumRecords() {
        return numRecords;
    }

    /**
     * Sets the number of records that are in the catalog.
     * 
     * @param numRecords The number of records
     */
    @JsonProperty("Num-Records")
    public void setNumRecords(long numRecords) {
        this.numRecords = numRecords;
    }

    /**
     * Gets the size of the catalog in bytes
     * 
     * @return The size in bytes
     */
    @JsonProperty("Size")
    public long getSize() {
        return size;
    }

    /**
     * Sets the size of the catalog in bytes
     * 
     * @param size The size in bytes
     */
    @JsonProperty("Size")
    public void setSize(long size) {
        this.size = size;
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static Catalog fromJson(String json) throws JsonProcessingException { 
        if (json == null){
            return null;
        }
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        Catalog record = mapper.readValue(json, Catalog.class);
        return record;

    }
    
    /**
     * Converts a JSON representation of a list of catalogs to a list of @{link Catalog} objects.
     * 
     * @param json The json array of catalog objects
     * @return The converted list of Catalog objects
     * @throws JsonProcessingException When reading JSON string fails
     */
    public static List<Catalog> fromJsonArray(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        List<Catalog> returnList = new ArrayList<>();
        if (json.trim().startsWith("[")){
            DotNotationList list = DotNotationList.fromJson(json);
            for(Object obj : list){
                if (obj instanceof DotNotationMap){
                    DotNotationMap map = (DotNotationMap)obj;
                    DotNotationMap schema = map.getProperty("Schema");
                    map.remove("Schema");
                    Catalog c = mapper.readValue(((DotNotationMap)obj).toJson(), Catalog.class);
                    if (schema != null){
                        c.setSchema(JsonSchema.fromJson(schema.toJson()));
                    }
                    returnList.add(mapper.readValue(((DotNotationMap)obj).toJson(), Catalog.class));
                }
            }
        }else{
            Catalog record = mapper.readValue(json, Catalog.class);
            returnList.add(record);
        }
        
        return returnList;
    }
    
    
    
}
