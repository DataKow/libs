package org.datakow.catalogs.metadata.webservice;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.catalogs.metadata.database.MetadataDataCoherence;
import org.datakow.catalogs.metadata.jsonpatch.JsonPatchOperation;
import org.datakow.catalogs.metadata.webservice.configuration.MyRestTemplate;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.CloseableIterator;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.IteratorToInputStream;
import org.datakow.core.components.JsonInputStreamToIterator;
import org.datakow.core.components.DatakowObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.logging.log4j.ThreadContext;
import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.Catalog;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

/**
 * Client used to interact with records in the Metadata Catalog Web Service
 * 
 * @author kevin.off
 */
public class MetadataCatalogWebserviceClient {

    MyRestTemplate template;
    String baseUrl;
    String userName;
    String password;

    public void setUsername(String username){
        this.userName = username;
    }
    
    public void setPassword(String password){
        this.password = password;
    }
    
    /**
     * Creates an instance with all of the necessary information.
     * 
     * @param template The underlying RestTemplate to use to make requests
     * @param baseUrl The Base URL of the metadata catalog web service
     * @param userName The username to use to make the requests
     * @param password The password to use to make the requests
     */
    public MetadataCatalogWebserviceClient(MyRestTemplate template, String baseUrl, String userName, String password){
        this.template = template;
        this.baseUrl = baseUrl;
        this.userName = userName;
        this.password = password;
    }
    
    /**
     * Gets a record by its ID
     * 
     * @param catalogIdentifier The catalog to retrieve the record from
     * @param recordIdentifier The ID of the record to retrieve
     * @param properties Properties that you want back in the record. Null for all.
     * @param coherence The desired data coherence
     * @return The record or null
     * @throws JsonProcessingException When there is an error parsing the JSON response
     * @throws ResourceAccessException When an IOException occurs while communicating with the server
     * @throws RestClientResponseException If a response from the server is something other than 200, 201, or 404
     */
    public MetadataCatalogRecord getById(
            String catalogIdentifier, String recordIdentifier, List<String> properties, MetadataDataCoherence coherence) 
            throws JsonProcessingException, ResourceAccessException, RestClientResponseException{
        
        setupCorrelationId();

        URI requestUri = MetadataCatalogWebserviceRequest.builder()
                .withDataCoherence(coherence)
                .withProjectionProperties(properties)
                .toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier);
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        String requestId = request.getHeaders().getFirst("X-Request-ID");
        
        try{
            ThreadContext.put("subRequestId", requestId);
            Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                    Level.INFO, "Sending request getById {0}", 
                    new Object[]{requestUri});


            ResponseEntity<String> response = template.exchange(
                    requestUri, 
                    HttpMethod.GET, 
                    request, 
                    String.class); 

            //only possible responses are 200, 201, and 404. Everything else is an exception
            if(response.getStatusCode() == HttpStatus.OK){
                MetadataCatalogRecord record = MetadataCatalogRecord.fromJson(response.getBody());
                return record;
            }else{
                return null;
            }
        }finally{
            ThreadContext.remove("subRequestId");
        }
    }
    
    public CloseableIterator<MetadataCatalogRecord> getByQueryAndStream(
            String catalogIdentifier, MetadataCatalogWebserviceRequest webServiceRequest) 
            throws JsonParseException, RestClientResponseException, ResourceAccessException{
        
        setupCorrelationId();
        URI requestUrl = webServiceRequest.toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records");
        HttpHeaders headers = getRequiredHeaders();
        
        try{
            ThreadContext.put("subRequestId", headers.getFirst("X-Request-ID"));
            Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(Level.INFO, "Sending request getByQuery {0}", requestUrl);
            
            HttpEntity request = new HttpEntity(headers);
            return template.exchangeForIterator(requestUrl, HttpMethod.GET, request, MetadataCatalogRecord.class, false);
        
        }finally{
            ThreadContext.remove("subRequestId");
        }
        
    }
    
    public JsonInputStreamToIterator<Object> distinct(String catalogIdentifier, String distinctProperty, String fiql, MetadataDataCoherence coherence) 
            throws JsonProcessingException, RestClientResponseException, ResourceAccessException, IOException{
        
        setupCorrelationId();
        
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        try{
            ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
            Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                    Level.INFO, "Sending request get distinct. catalog: {0}, distinct: {1}, fiql: {2}, coherence: {3}", 
                    new Object[]{catalogIdentifier, distinctProperty, fiql, coherence.getCoherenceName()});


            URI requestUrl = MetadataCatalogWebserviceRequest.builder()
                    .withQuery(fiql)
                    .withDistinct(distinctProperty)
                    .withDataCoherence(coherence)
                    .toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/distinct");

            return template.exchangeForIterator(requestUrl, HttpMethod.GET, request, Object.class, false);
        }finally{
            ThreadContext.remove("subRequestId");
        }
    }
    
    public Integer count(String catalogIdentifier, String fiql, int limit, MetadataDataCoherence coherence) 
            throws JsonProcessingException, RestClientResponseException, ResourceAccessException{
        
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        try{
            ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
            Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                    Level.INFO, "Sending request get count. catalog: {0}, fiql: {1}, limit: {2}, coherence: {3}", 
                    new Object[]{catalogIdentifier, fiql, limit, coherence.getCoherenceName()});
            

            URI requestUrl = MetadataCatalogWebserviceRequest.builder()
                    .withQuery(fiql)
                    .withLimit(limit)
                    .withDataCoherence(coherence)
                    .toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/count");

            ResponseEntity<String> response = template.exchange(
                    requestUrl,
                    HttpMethod.GET, 
                    request, 
                    String.class);

            if (response.getStatusCode() != HttpStatus.NOT_FOUND){
                DotNotationMap returnVal = DotNotationMap.fromJson(response.getBody());
                return returnVal.getProperty("Num-Records");
            }else{
                return null;
            }
        }finally{
            ThreadContext.remove("subRequestId");
        }
    }
   
    
    /**
     * Performs an update on a record by replacing the document.
     * 
     * @param catalogIdentifier The catalog identifier
     * @param recordIdentifier The ID of the record to update
     * @param realm The new security realm
     * @param tags A new list of tags
     * @param document The document to replace it with
     * @param identities A list of associated object identities
     * @return True on success false on 404
     * @throws JsonProcessingException When there is an error converting the document to JSON
     * @throws ResourceAccessException When an IOException occurs while communicating with the server
     * @throws RestClientResponseException If a response from the server is something other than 200, 201, or 404
     */
    public boolean updateOneById(
            String catalogIdentifier, String recordIdentifier, String realm, List<String> tags, 
            DotNotationMap document, CatalogIdentityCollection identities) 
            throws JsonProcessingException, ResourceAccessException, RestClientResponseException{
        
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        if (realm != null && !realm.isEmpty()){
            headers.set("realm", realm);
        }
        if (tags != null && tags.size() > 0){
            headers.set("Tags", String.join(",", tags));
        }
        if (identities != null && identities.size() > 0){
            headers.set("Object-Identities", identities.toHttpHeader());
        }
        
        HttpEntity<String> request = new HttpEntity<>(document.toJson(), headers);
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request update {0}:{1}. realm: {2}, tags {3}", 
                new Object[]{catalogIdentifier, recordIdentifier, realm, tags});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier, 
                HttpMethod.PUT, 
                request, 
                String.class);
        
        return response.getStatusCode() == HttpStatus.OK;
    }
    
    /**
     * Performs an update on a record by replacing the document.
     * 
     * @param catalogIdentifier The catalog identifier
     * @param fiql Query to use to find records to update
     * @param sortString Sort string used to sort records as the first one will be updated
     * @param realm The new security realm
     * @param tags A new list of tags
     * @param document The document to replace it with
     * @param identities Associated object identities
     * @return True if the record was updated, false if it was not found
     * @throws JsonProcessingException When there is an error converting the document to JSON
     * @throws ResourceAccessException When an IOException occurs while communicating with the server
     * @throws RestClientResponseException If a response from the server is something other than 200, 201, or 404
     */
    public boolean updateOneByQuery(
            String catalogIdentifier, String fiql, String sortString, String realm, List<String> tags, 
            DotNotationMap document, CatalogIdentityCollection identities)
            throws JsonProcessingException, ResourceAccessException, RestClientResponseException{
        
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        if (realm != null && !realm.isEmpty()){
            headers.set("realm", realm);
        }
        if (tags != null && tags.size() > 0){
            headers.set("Tags", String.join(",", tags));
        }
        if (identities != null && identities.size() > 0){
            headers.set("Object-Identities", identities.toHttpHeader());
        }
        HttpEntity<String> request = new HttpEntity<>(document.toJson(), headers);
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request udpate {0}: fiql: {1}, realm: {2}, tags {3}", 
                new Object[]{catalogIdentifier, fiql, realm, tags});
        ThreadContext.remove("subRequestId");
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withSort(sortString)
                .toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records");
        
        ResponseEntity<String> response = template.exchange(
                uri, 
                HttpMethod.PUT, 
                request, 
                String.class);
        
        return response.getStatusCode().equals(HttpStatus.OK);
    }
    
    /**
     * Deletes a record from a catalog 
     * 
     * @param catalogIdentifier The catalog identifier
     * @param recordIdentifier The id of the record to delete
     * @return true on success
     * @throws ResourceAccessException When an IOException occurs while communicating with the server
     * @throws RestClientResponseException If a response from the server is something other than 200, 201, or 404
     */
    public boolean deleteById(String catalogIdentifier, String recordIdentifier) 
            throws ResourceAccessException, RestClientResponseException{
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request delete {0}:{1}]", 
                new Object[]{catalogIdentifier, recordIdentifier});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier, 
                HttpMethod.DELETE, 
                request, 
                String.class);
        
        return response.getStatusCode() == HttpStatus.OK;
    }
    
    /**
     * Deletes records from a catalog that match the query.
     * 
     * @param catalogIdentifier The catalog identifier
     * @param fiql The query to use to find records to delete
     * @return The number of records deleted or -1 if unknown
     * @throws RestClientException If there is a return status code other than 200, 201, or 404
     * @throws ResourceAccessException If there is an IO Exception during the request
     */
    public int deleteByQuery(String catalogIdentifier, String fiql) 
            throws ResourceAccessException, RestClientResponseException {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request deleteByQuery {0}:{1}", 
                new Object[]{catalogIdentifier, fiql});
        ThreadContext.remove("subRequestId");
        
        URI uri = MetadataCatalogWebserviceRequest.builder().withQuery(fiql).toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records");
        
        ResponseEntity<String> response = template.exchange(
                uri, 
                HttpMethod.DELETE, 
                request, 
                String.class);
        
        //Response can be 200 or 404
        if (response.getStatusCode() == HttpStatus.OK){
            String numDeleted = response.getHeaders().getFirst("Num-Deleted");
            if (StringUtils.hasText(numDeleted)){
                return Integer.parseInt(numDeleted);
            }else{
                return -1;
            }
        }else{
            Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(Level.SEVERE, "The delete request for {0} resulted in a {1} return status.", new Object[]{fiql, response.getStatusCodeValue()});
            return 0;
        }
    }

    public CatalogIdentity create(
        String catalogIdentifier, 
        DotNotationMap document) 

        throws JsonProcessingException, ResourceAccessException, RestClientResponseException {
            return create(catalogIdentifier, UUID.randomUUID().toString(), null, null, document, null);
    }
    
    public CatalogIdentity create(
        String catalogIdentifier, 
        DotNotationMap document, 
        CatalogIdentityCollection identities) 

        throws JsonProcessingException, ResourceAccessException, RestClientResponseException {
            return create(catalogIdentifier, UUID.randomUUID().toString(), null, null, document, identities);
    }

    public CatalogIdentity create(
        String catalogIdentifier, 
        String realm, 
        List<String> tags, 
        DotNotationMap document, 
        CatalogIdentityCollection identities) 

        throws JsonProcessingException, ResourceAccessException, RestClientResponseException {
            return create(catalogIdentifier, UUID.randomUUID().toString(), tags, document, identities);
    }

    /**
     * Creates a new record in a catalog
     * @param catalogIdentifier The catalog identifier
     * @param recordIdentifier The ID of the record you want to set
     * @param realm The security realm to set
     * @param tags The list of tags to set
     * @param document The document to set
     * @param objectIdentities The associated object records to set
     * @return A response with information regarding the status of the operation
     * @throws JsonProcessingException If converting the document to JSON fails
     * @throws RestClientException If there is a return status code other than 201
     * @throws ResourceAccessException If there is an IO Exception during the request
     */
    public CatalogIdentity create(
            String catalogIdentifier, String recordIdentifier, String realm, List<String> tags, DotNotationMap document, 
            CatalogIdentityCollection objectIdentities) 
            throws JsonProcessingException, ResourceAccessException, RestClientResponseException {
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        if (realm != null && !realm.isEmpty()){
            headers.set("realm", realm);
        }
        if (tags != null && tags.size() > 0){
            headers.set("Tags", String.join(",", tags));
        }
        if (objectIdentities != null && objectIdentities.size() > 0){
            headers.set("Object-Identities", objectIdentities.toHttpHeader());
        }
        if (StringUtils.hasText(recordIdentifier)){
            headers.set("Record-Identifier", recordIdentifier);
        }
        
        HttpEntity<String> request = new HttpEntity<>(document.toJson(), headers);
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request create {0}", 
                new Object[]{catalogIdentifier});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs/" + catalogIdentifier + "/records", 
                HttpMethod.POST, 
                request, 
                String.class);
        
        if (response.getStatusCode() == HttpStatus.CREATED){
            CatalogIdentity identity;
            if (response.getHeaders().containsKey(HttpHeaders.LOCATION)){
                identity = CatalogIdentity.fromUrl(response.getHeaders().getFirst(HttpHeaders.LOCATION));
            }else{
                identity = new CatalogIdentity(catalogIdentifier, DotNotationMap.fromJson(response.getBody()).getProperty("id"));
            }
            return identity;
        }else{
            //Throw the 404 as an exception
            throw new HttpClientErrorException(response.getStatusCode(), 
                    response.getStatusCode().getReasonPhrase(), response.getHeaders(), 
                    response.getBody().getBytes(), getResponseCharset(response));
        }
    }
    
    /**
     * Uses MongoDB's bulk operations to create records.
     * 
     * @param catalogIdentifier The identifier of the catalog to create the records in.
     * @param realm The security realm to apply to all records if one is not specified specifically
     * @param tags The tags to apply to all records if one is not specified specifically
     * @param documents A list of {@link MetadataCatalogRecord} objects
     * @param identities The associated records
     * @return In iterator of the bulk results
     * @throws JsonProcessingException If converting the document to JSON fails
     * @throws RestClientException If there is a return status code other than 200
     * @throws ResourceAccessException If there is an IO Exception during the request
     */
    public JsonInputStreamToIterator<BulkResult> createBulk(
            String catalogIdentifier, String realm, List<String> tags, Collection<MetadataCatalogRecord> documents, 
            CatalogIdentityCollection identities) 
            throws ResourceAccessException, RestClientResponseException, JsonProcessingException, IOException{
        
        return createBulk(catalogIdentifier, realm, tags, IteratorToInputStream.jsonProducerIteratorToJsonArrayInputStream(documents.iterator()), identities);

    }
    
    /**
     * Uses MongoDB's bulk operations to create records.
     * 
     * @param catalogIdentifier The identifier of the catalog to create the records in.
     * @param realm The security realm to apply to all records if one is not specified specifically
     * @param tags The tags to apply to all records if one is not specified specifically
     * @param documents An input stream with a JSON List of {@link MetadataCatalogRecord} objects
     * @param identities The associated records
     * @return An iterator of the bulk result
     * @throws RestClientException If there is a return status code other than 200
     * @throws ResourceAccessException If there is an IO Exception during the request
     * @throws com.fasterxml.jackson.core.JsonProcessingException If there is a problem converting json to a bulk result
     */
    public JsonInputStreamToIterator<BulkResult> createBulk(
            String catalogIdentifier, String realm, List<String> tags, 
            InputStream documents, CatalogIdentityCollection identities) 
            throws ResourceAccessException, RestClientResponseException, JsonProcessingException{
        
        setupCorrelationId();
        
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        headers.set("Operation-Type", "bulk");
        if (realm != null && !realm.isEmpty()){
            headers.set("realm", realm);
        }
        if (tags != null && tags.size() > 0){
            headers.set("Tags", String.join(",", tags));
        }
        if (identities != null && identities.size() > 0){
            headers.set("Object-Identities", identities.toHttpHeader());
        }
        InputStreamResource resource = new InputStreamResource(documents);
        HttpEntity<InputStreamResource> request = new HttpEntity<>(resource, headers);
        URI url = MetadataCatalogWebserviceRequest.builder().toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records");
        
        try{
            ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
            Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                    Level.INFO, "Sending request createBulk {0}", 
                    new Object[]{catalogIdentifier});
            
            return template.exchangeForIterator(url, HttpMethod.POST, request, BulkResult.class, true);
        }finally{
            ThreadContext.remove("subRequestId");
        }
    }
    
    /**
     * Uses MongoDB's bulk operations to create records.
     * 
     * @param catalogIdentifier The identifier of the catalog to create the records in.
     * @param filter FIQL filter query used to find documents based on properties within the given documents
     * @param realm The security realm to apply to all records if one is not specified specifically
     * @param tags The tags to apply to all records if one is not specified specifically
     * @param records An input stream with a JSON List of {@link MetadataCatalogRecord} objects
     * @param multi Whether each individual update should update more than 1 document
     * @param identities The associated records
     * @return An iterator of the bulk result
     * @throws RestClientException If there is a return status code other than 200
     * @throws ResourceAccessException If there is an IO Exception during the request
     * @throws com.fasterxml.jackson.core.JsonProcessingException If there is a problem converting json to a bulk result
     */
    public JsonInputStreamToIterator<BulkResult> updateBulkByParameterizedFilter(
            String catalogIdentifier, String filter, String realm, List<String> tags, 
            InputStream records, boolean multi, CatalogIdentityCollection identities) 
            throws ResourceAccessException, RestClientResponseException, JsonProcessingException{
        
        setupCorrelationId();
        
        HttpHeaders headers = getRequiredHeaders();
        headers.setContentType(new MediaType("application", "json", StandardCharsets.UTF_8));
        headers.set("Operation-Type", "bulk");
        if (realm != null && !realm.isEmpty()){
            headers.set("realm", realm);
        }
        if (tags != null && tags.size() > 0){
            headers.set("Tags", String.join(",", tags));
        }
        if (identities != null && identities.size() > 0){
            headers.set("Object-Identities", identities.toHttpHeader());
        }
        InputStreamResource resource = new InputStreamResource(records);
        HttpEntity<InputStreamResource> request = new HttpEntity<>(resource, headers);
        URI url = MetadataCatalogWebserviceRequest.builder()
                .withFilter(filter)
                .withMulti(multi)
                .toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records");
        
        try{
            ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
            Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                    Level.INFO, "Sending request createBulk {0}", 
                    new Object[]{catalogIdentifier});
            
            return template.exchangeForIterator(url, HttpMethod.PUT, request, BulkResult.class, true);
        }finally{
            ThreadContext.remove("subRequestId");
        }
    }

    public boolean jsonPatchById(String catalogIdentifier, String id, List<JsonPatchOperation> jsonPatch) throws JsonProcessingException{
        
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records/" + id);
        
        HttpHeaders headers = getRequiredHeaders();
        headers.set("Content-Type", "application/json-patch+json");
        
        
        HttpEntity<String> request = new HttpEntity<>(DatakowObjectMapper.getDatakowObjectMapper().writeValueAsString(jsonPatch), headers);
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request mergePatch {0}", 
                new Object[]{catalogIdentifier});
        ThreadContext.remove("subRequestId");

        ResponseEntity<String> response = template.exchange(uri, HttpMethod.PATCH, request, String.class);
        
        return response.getStatusCodeValue() == 200;

    }
    
    public JsonInputStreamToIterator<BulkResult> jsonPatchByQuery(String catalogIdentifier, 
            String fiql, String sort, int limit, List<JsonPatchOperation> jsonPatch,  boolean upsert) throws JsonProcessingException{
        
        URI url = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withUpsert(upsert)
                .withSort(sort)
                .withLimit(limit)
                .toUri(baseUrl + "/catalogs/" + catalogIdentifier + "/records");
        
        HttpHeaders headers = getRequiredHeaders();
        headers.set("Content-Type", "application/json-patch+json");
        
        
        HttpEntity<String> request = new HttpEntity<>(DatakowObjectMapper.getDatakowObjectMapper().writeValueAsString(jsonPatch), headers);
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request mergePatch {0}", 
                new Object[]{catalogIdentifier});
        ThreadContext.remove("subRequestId");

        return template.exchangeForIterator(url, HttpMethod.PATCH, request, BulkResult.class, true);

    }
    
    public boolean mergePatchById(String catalogIdentifier, String id, DotNotationMap mergePatch) throws JsonProcessingException{
        
        String url = MetadataCatalogWebserviceRequest.builder()
                .toUrl(baseUrl + "/catalogs/" + catalogIdentifier + "/records/" + id);
        
        HttpHeaders headers = getRequiredHeaders();
        headers.set("Content-Type", "application/merge-patch+json");
        
        
        HttpEntity<String> request = new HttpEntity<>(mergePatch.toJson(), headers);
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request mergePatch {0}", 
                new Object[]{catalogIdentifier});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(
                url, 
                HttpMethod.PATCH, 
                request, 
                String.class);
        
        return response.getStatusCode().equals(HttpStatus.OK);
    }
    
    public int mergePatchByQuery(String catalogIdentifier, String fiql, String sort, DotNotationMap mergePatch, boolean updateMulti, boolean upsert) throws JsonProcessingException{
        
        String uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withMulti(updateMulti)
                .withUpsert(upsert)
                .withSort(sort)
                .toUrl(baseUrl + "/catalogs/" + catalogIdentifier + "/records");
        
        HttpHeaders headers = getRequiredHeaders();
        headers.set("Content-Type", "application/merge-patch+json");
        
        
        HttpEntity<String> request = new HttpEntity<>(mergePatch.toJson(), headers);
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request mergePatch {0}", 
                new Object[]{catalogIdentifier});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(
                uri, 
                HttpMethod.PATCH, 
                request, 
                String.class);
        
        return Integer.parseInt(response.getHeaders().getFirst("Num-Updated"));
    }
    
    /**
     * Gets a list of all catalog identifiers in the Metadata Catalog
     * @return The list of catalog identifiers
     * @throws JsonProcessingException When reading JSON string fails 
     * @throws RestClientException If there is a return status code other than 200, 201
     * @throws ResourceAccessException If there is an IO Exception during the request
     */
    public List<String> getCatalogNames() 
            throws JsonProcessingException, ResourceAccessException, RestClientResponseException {
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(MetadataCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request getCatalogNames");
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(
                baseUrl + "/catalogs", 
                HttpMethod.GET, 
                request, 
                String.class);
        
        if (response.getStatusCode() == HttpStatus.OK){
            List<Catalog> catalogs = Catalog.fromJsonArray(response.getBody());
            return catalogs
                    .stream()
                    .map(Catalog::getCatalogIdentifier)
                    .collect(Collectors.toList());
        }else{
            //Throw the 404 as an exception
            throw new HttpClientErrorException(response.getStatusCode(), 
                    response.getStatusCode().getReasonPhrase(), response.getHeaders(), 
                    response.getBody().getBytes(), getResponseCharset(response));
        }
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
        if (correlationId == null || correlationId.isEmpty()){
            correlationId = UUID.randomUUID().toString();
            ThreadContext.put("correlationId", correlationId);
        }
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
    
    /**
     * Gets the charset from the response.
     * 
     * @param response The response
     * @return The charset of the response
     */
    private Charset getResponseCharset(ResponseEntity response){
        HttpHeaders headers = response.getHeaders();
        MediaType contentType = headers.getContentType();
        Charset charset = contentType != null ? contentType.getCharset() : null;
        return  charset;
    }
    
}
