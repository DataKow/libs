package org.datakow.catalogs.metadata.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.catalogs.metadata.indexes.MongoIndex;
import org.datakow.catalogs.metadata.jsonschema.JsonSchema;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.logging.log4j.ThreadContext;
import org.datakow.catalogs.metadata.Catalog;
import org.datakow.catalogs.metadata.DataRetentionPolicy;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * Web service client that interacts with the Metadata Catalog Web Service Client
 * to perform administrative operations. 
 * 
 * @author kevin.off
 */
public class MetadataCatalogManagementWebserviceClient {

    
    RestTemplate template;
    String baseUrl;
    String userName;
    String password;
    
    /**
     * Creates an instance with all of the necessary information.
     * 
     * @param template The underlying RestTemplate to use to make the requests
     * @param baseUrl The base URL of the Metadata Catalog Web Service Client
     * @param userName The username to use for the requests
     * @param password The password to use for the requests
     */
    public MetadataCatalogManagementWebserviceClient(RestTemplate template, String baseUrl, String userName, String password){
        this.template = template;
        this.baseUrl = baseUrl;
        this.userName = userName;
        this.password = password;
    }
    
    /**
     * Gets a catalog's information by the virtual catalog identifier.
     * 
     * @param catalogIdentifier The identifier of the catalog
     * @param includeIndexes To include indexes or not
     * @param includeStats To include count and size or not
     * @return The catalog's information
     * @throws JsonProcessingException When reading JSON string fails
     */
    public Catalog getCatalogByCatalogIdentifier(String catalogIdentifier, boolean includeIndexes, boolean includeStats) throws JsonProcessingException {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        String indexes = includeIndexes ? "true" : "false";
        String stats = includeStats ? "true" : "false";
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "?includeIndexes=" + indexes + "&includeStats=" + stats, 
                HttpMethod.GET, 
                request, 
                String.class);
        Catalog catalog = Catalog.fromJson(response.getBody());
        return catalog;
        
    }
    
    /**
     * Gets information for all catalogs within the Metadata Catalog Web Service
     * 
     * @param includeIndexes true to include index information for the catalogs. Can be slower and more verbose.
     * @param includeStats true to include size and count
     * @return A list of all of the catalog's information
     * @throws JsonProcessingException When reading JSON string fails 
     */
    public List<Catalog> getAllCatalogs(boolean includeIndexes, boolean includeStats) throws JsonProcessingException {
        
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        String indexes = includeIndexes ? "true" : "false";
        String stats = includeStats ? "true" : "false";
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/?includeIndexes=" + indexes + "&includeStats=" + stats, 
                HttpMethod.GET, 
                request, 
                String.class);
        List<Catalog> catalogs = Catalog.fromJsonArray(response.getBody());
        return catalogs;
        
    }
    
    /**
     * Creates a new catalog in the Metadata Catalog Web Service. 
     * <p>
     * Can also be used to modify existing catalogs, indexes, create collections, or create additional data retention policies.
     * 
     * @param catalogIdentifier The Virtual Catalog Identifier to use
     * @param collectionName The name of the underlying MongoDB collection
     * @param indexStorageObject true to create default indexes on all properties in the storage object
     * @param setDefaultRetentionPolicy true to add the default 7 day retention policy
     * @param createCollection true to create the underlying MongoDB collection
     * @return true on success or exception
     */
    public boolean createCatalog(
            String catalogIdentifier, 
            String collectionName, 
            boolean indexStorageObject, 
            boolean setDefaultRetentionPolicy, 
            boolean createCollection){
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        HttpEntity<String> request = new HttpEntity<>(headers);
        String queryString = "Catalog-Identifier=" + catalogIdentifier;
        queryString += "&indexStorageObject=" + (indexStorageObject ? "true" : "false");
        queryString += "&setDefaultRetentionPolicy=" + (setDefaultRetentionPolicy ? "true" : "false");
        queryString += "&createCollection=" + (createCollection ? "true" : "false");
        
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/?" + queryString, 
                HttpMethod.POST, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.CREATED;

    }

    /**
     * Deletes a catalog by removing the catalog's information and deleting the collection.
     * 
     * @param catalogIdentifier The catalog identifier of the catalog to remove
     * @return true on success or exception
     */
    public boolean deleteCatalog(String catalogIdentifier) {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier, 
                HttpMethod.DELETE, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.OK;
        
    }

    /**
     * Gets the indexes of a catalog
     * 
     * @param catalogIdentifier The catalog to get the indexes for
     * @return A list of indexes
     * @throws JsonProcessingException When reading JSON string fails
     * @throws IOException When reading JSON string fails 
     */
    public List<MongoIndex> getIndexes(String catalogIdentifier) throws JsonProcessingException, IOException {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/indexes", 
                HttpMethod.GET, 
                request, 
                String.class);
        List<MongoIndex> indexes = MongoIndex.fromJson(response.getBody());
        return indexes;
        
    }
    
    /**
     * Creates an index in MongoDB.
     * 
     * @param catalogIdentifier The catalog to create the index for
     * @param index The index definition
     * @return True on success
     * @throws IOException This is here by error and should be removed
     */
    public boolean createIndex(String catalogIdentifier, MongoIndex index) throws IOException {
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        HttpEntity<String> request = new HttpEntity<>(index.toJson(), headers);
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/indexes", 
                HttpMethod.POST, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.CREATED;
        
    }

    /**
     * Deletes an index in a MongoDB collection
     * 
     * @param catalogIdentifier The catalog identifier of the catalog to delete the index from
     * @param indexName The name of the index to delete. This may not the the same as the name of the property that the index is on.
     * @return true on success
     */
    public boolean deleteIndex(String catalogIdentifier, String indexName) {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/indexes/" + indexName, 
                HttpMethod.DELETE, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.OK;
        
    }

    /**
     * Deletes all indexes for a catalog
     * 
     * @param catalogIdentifier The catalog to delete the indexes for
     * @return true for success
     */
    public boolean deleteAllIndexes(String catalogIdentifier) {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/indexes", 
                HttpMethod.DELETE, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.OK;
        
    }

    /**
     * Gets the schema for a catalog
     * 
     * @param catalogIdentifier The catalog to get the schema for
     * @return The schema
     * @throws IOException When reading JSON string fails
     * @throws JsonProcessingException When reading JSON string fails 
     */
    public JsonSchema getSchema(String catalogIdentifier) throws IOException, JsonProcessingException {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/schema", 
                HttpMethod.GET, 
                request, 
                String.class);
        JsonSchema schema = JsonSchema.fromJson(response.getBody());
        return schema;

    }
    
    /**
     * Saves a schema object for a catalog. 
     * <p>
     * This will replace the existing schema if one already exists.
     * 
     * @param catalogIdentifier The catalog to save the schema to
     * @param schema The schema to save.
     * @return true on success
     * @throws IOException This is here by mistake and should be removed
     */
    public boolean saveSchema(String catalogIdentifier, JsonSchema schema) throws IOException {
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        HttpEntity<String> request = new HttpEntity<>(schema.toJson(), headers);
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/schema", 
                HttpMethod.POST, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.CREATED;
        
    }

    /**
     * Deletes the schema on a catalog.
     * 
     * @param catalogIdentifier the catalog to delete the schema from
     * @return true on success
     */
    public boolean deleteSchema(String catalogIdentifier) {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/schema", 
                HttpMethod.DELETE, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.OK;
        
    }
    
    /**
     * Gets the list of data retention policies set on a catalog.
     * 
     * @param catalogIdentifier The catalog identifier
     * @return The list of policies
     * @throws IOException When reading JSON string fails
     * @throws JsonProcessingException When reading JSON string fails 
     */
    public List<DataRetentionPolicy> getDataRetentionPolicy(String catalogIdentifier) throws IOException, JsonProcessingException {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/retention", 
                HttpMethod.GET, 
                request, 
                String.class);
        DotNotationList policyList = DotNotationList.fromJson(response.getBody());
        List<DataRetentionPolicy> policies = new ArrayList<>();
        for(Object policyObj : policyList){
            if (policyObj instanceof DotNotationMap){
                policies.add(DataRetentionPolicy.fromJson(((DotNotationMap)policyObj).toJson()));
            }
        }
        return policies;

    }
    
    /**
     * Saves a list of data retention policies to a catalog.
     * <p>
     * The retention policy will be overwritten if on already exists.
     * 
     * @param catalogIdentifier The catalog the store the policies in
     * @param policies The policies to save
     * @return True on success
     * @throws IOException This is here by mistake and should be removed
     */
    public boolean saveDataRetentionPolicy(String catalogIdentifier, List<DataRetentionPolicy> policies) throws IOException {
        setupCorrelationId();
        DotNotationList policyList = new DotNotationList(policies);
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        HttpEntity<String> request = new HttpEntity<>(policyList.toJson(), headers);
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/retention", 
                HttpMethod.POST, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.CREATED;
        
    }

    /**
     * Deletes all data retention policy objects from a catalog.
     * 
     * @param catalogIdentifier The catalog to delete the policies for.
     * @return True on success
     */
    public boolean deleteDataRetentionPolicy(String catalogIdentifier) {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/retention", 
                HttpMethod.DELETE, 
                request, 
                String.class);
        return response.getStatusCode() == HttpStatus.OK;
        
    }

    /**
     * Gets a list of all of the catalog identifiers in the system.
     * 
     * @return The list of all catalog identifiers
     * @throws IOException On a Mongo error
     */
    public List<String> getMetadataCatalogIdentifiers() throws IOException {
        setupCorrelationId();
        List<Catalog> catalogs = getAllCatalogs(false, false);
        return catalogs.stream()
                .filter(c->c.getCatalogType().equalsIgnoreCase("metadata"))
                .map(c->c.getCatalogIdentifier())
                .collect(Collectors.toList());
    }

    /**
     * Gets the headers that are required for every request.
     * <p>
     * This includes the correlationId and request id
     * @return 
     */
    private HttpHeaders getRequiredHeaders(){
        HttpHeaders headers = new HttpHeaders();
        if (userName != null && !userName.isEmpty()){
            headers.set(HttpHeaders.AUTHORIZATION, "Basic " + DatatypeConverter.printBase64Binary((userName+":"+password).getBytes()));
        }
        String correlationId = ThreadContext.get("correlationId");
        String requestId = UUID.randomUUID().toString();
        headers.set("X-Correlation-ID", correlationId);
        headers.set("X-Request-ID", requestId);
        return headers;
    }
    
    /**
     * Adds a correlation ID to the log4j {@link ThreadContext}
     * if it doesn't already exist.
     */
    private void setupCorrelationId(){
        String correlationId = ThreadContext.get("correlationId");
        if (correlationId == null || correlationId.isEmpty()){
            ThreadContext.put("correlationId", correlationId);
        }
    }
    
}
