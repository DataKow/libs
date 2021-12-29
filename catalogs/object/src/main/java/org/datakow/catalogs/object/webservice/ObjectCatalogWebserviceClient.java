package org.datakow.catalogs.object.webservice;


import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.catalogs.object.database.ObjectDataCoherence;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationMap;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.logging.log4j.ThreadContext;
import org.datakow.catalogs.object.ObjectCatalogRecord;
import org.datakow.catalogs.object.ObjectCatalogRecordInput;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;

/**
 * Client to use to interact with the Object Catalog Web Service using its REST interface.
 * 
 * @author kevin.off
 */
public class ObjectCatalogWebserviceClient {

    RestTemplate template;
    String baseUrl;
    String userName;
    String password;
    
    /**
     * Creates an instance with all of the necessary components 
     * 
     * @param template The underlying RestTemplate used to make requests
     * @param baseUrl The base URL of the object catalog web service
     * @param userName The username to use
     * @param password The password to use
     */
    public ObjectCatalogWebserviceClient(RestTemplate template, String baseUrl, String userName, String password){
        this.template = template;
        this.baseUrl = baseUrl;
        this.userName = userName;
        this.password = password;
    }
    
    /**
     * Gets an object by its ID
     * 
     * @param catalogName The name of the object catalog to retrieve a record from
     * @param id The ID of the record
     * @param coherence The desired data coherence
     * @return The record that was retrieved or null
     * @throws ResourceAccessException If there is a problem communicating with the object catalog web service
     * @throws RestClientResponseException if the response is anything other than 200 or 404
     */
    public ObjectCatalogRecord getById(String catalogName, String id, ObjectDataCoherence coherence) 
            throws ResourceAccessException, RestClientResponseException{
        
        setupCorrelationId();
        
        URI uri = new DefaultUriBuilderFactory().expand(
                baseUrl + "/catalogs/" + catalogName + "/objects/" + id + "?dataCoherence={coherence}", 
                coherence.getCoherenceName());
        
        try{
        
            ClientHttpRequest request = template.getRequestFactory().createRequest(
                    uri, 
                    HttpMethod.GET);
            request.getHeaders().putAll(getRequiredHeaders());

            ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
            Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(
                    Level.INFO, "Sending request getById({0}, {1})", 
                    new Object[]{catalogName, id});
            ThreadContext.remove("subRequestId");

            ClientHttpResponse response = request.execute();

            if (!template.getErrorHandler().hasError(response)){
                if (response.getStatusCode() == HttpStatus.OK){

                    ObjectCatalogRecord record = new ObjectCatalogRecord();
                    if(response.getHeaders().containsKey("Record-Identifier")){
                        record.setId(response.getHeaders().getFirst("Record-Identifier"));
                    }
                    if(response.getHeaders().containsKey("Content-Length")){
                        record.setContentLength(Long.parseLong(response.getHeaders().getFirst("Content-Length")));
                    }
                    if(response.getHeaders().containsKey("Content-Type")){
                        record.setContentType(response.getHeaders().getFirst("Content-Type"));
                    }
                    if(response.getHeaders().containsKey("Content-MD5")){
                        record.setContentMD5(response.getHeaders().getFirst("Content-MD5"));
                    }
                    if (response.getHeaders().containsKey("Content-Encoding")){
                        record.setContentEncoding(response.getHeaders().getFirst("Content-Encoding"));
                    }
                    if (response.getHeaders().containsKey("Publisher")){
                        record.setPublisher(response.getHeaders().getFirst("Publisher"));
                    }
                    if (response.getHeaders().containsKey("Realm")){
                        record.setRealm(response.getHeaders().getFirst("Realm"));
                    }
                    if (response.getHeaders().containsKey("Tags")){
                        record.setTags(Arrays.asList(StringUtils.commaDelimitedListToStringArray(response.getHeaders().getFirst("Tags"))));
                    }
                    if (response.getHeaders().containsKey("Metadata-Catalog-Identifiers")){
                        record.setMetadataCatalogIdentifiers(StringUtils.commaDelimitedListToStringArray(response.getHeaders().getFirst("Metadata-Catalog-Identifiers")));
                    }
                    if (response.getHeaders().containsKey("Publish-Date")){
                        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
                        try {
                            Date d = format.parse(response.getHeaders().getFirst("Publish-Date"));
                            record.setPublishDate(d);
                        } catch (ParseException ex) {
                            Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(Level.SEVERE, 
                                    "The Publish-Date field is either missing or corrupt on record: " + catalogName + ":" + id, ex);
                        }
                        
                    }
                    if (response.getHeaders().containsKey("Metadata-Identities")){
                        String identity = response.getHeaders().getFirst("Metadata-Identities");
                        CatalogIdentityCollection collection = CatalogIdentityCollection.metadataAssociationFromHttpHeader(identity);
                        record.setObjectMetadataIdentities(collection);
                    }
                    
                    record.setData(response.getBody());
                    return record;
                }else{
                    response.close();
                    Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(Level.SEVERE, 
                            "The server responded with an error code ({0}) {1} on GET: {2}", 
                            new Object[]{response.getRawStatusCode(), response.getStatusText(), id});
                    return null;
                }
            }else{
                try{
                    template.getErrorHandler().handleError(response);
                }finally{
                    response.close();
                }
                return null;
            }
        }catch(IOException ex){
            throw new ResourceAccessException("An I/O Exception occurred while communicating with the Object Catalog Web Service", ex);
        }
    }
    
    /**
     * Deletes a record from the object catalog
     * @param catalogName The name of the catalog to delete a record from
     * @param id The id of the record
     * @throws ResourceAccessException on any kind of IOException
     * @throws RestClientResponseException on any status code other than 200, 201, or 404
     */
    public void delete(String catalogName, String id) 
            throws ResourceAccessException, RestClientResponseException{
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request delete({0}, {1})", 
                new Object[]{catalogName, id});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(baseUrl + "/catalogs/" + catalogName + "/objects/" + id, 
                HttpMethod.DELETE, 
                request, 
                String.class);

        //Only responses can be a 200 or a 404
        if (response.getStatusCode() != HttpStatus.OK){
            Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(Level.WARNING, 
                    "Server responded with status {0} {1} for delete {2}", 
                    new Object[]{response.getStatusCode().name(), response.getStatusCode().getReasonPhrase(), id});
        }
    }
    
    
    public int deleteByQuery(String catalogName, String fiql, int limit) 
            throws ResourceAccessException, RestClientResponseException{
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());

        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request deleteByQuery catalog: {0}, fiql: {1}, limit: {2}", 
                new Object[]{catalogName, fiql, limit});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(baseUrl + "/catalogs/" + catalogName + "/objects?s=" + fiql + "&limit=" + limit, 
                HttpMethod.DELETE, 
                request, 
                String.class);

        //Only responses can be a 200 or a 404
        if (response.getStatusCode() != HttpStatus.OK){
            Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(Level.WARNING, 
                    "Server responded with status {0} {1} for delete {2}", 
                    new Object[]{response.getStatusCode().name(), response.getStatusCode().getReasonPhrase(), fiql});
        }
        
        String numUpdated = response.getHeaders().getFirst("Num-Deleted");
        if (StringUtils.hasText(numUpdated)){
            return Integer.parseInt(numUpdated);
        }else{
            return -1;
        }
    }
    
    /**
     * Gets a list of Object ID's based on a query
     * 
     * @param catalogName The name of the catalog
     * @param fiql the fiql query
     * @param collectionFormat The format you want your collection to be represented (this method is not ready for anything other than legacy)
     * @param coherence The desired data coherence
     * @return A list of the IDS
     * @throws ResourceAccessException On any IO type Exception
     * @throws RestClientResponseException On any response code other than 200, 201, or 404
     */
    public List<String> getByQuery(String catalogName, String fiql, String collectionFormat, ObjectDataCoherence coherence) 
            throws ResourceAccessException, RestClientResponseException{
        return getByQuery(catalogName, fiql, -1, collectionFormat, coherence);
    }

     public List<String> getByQuery(
            String catalogName, String fiql, int limit, 
            String collectionFormat, ObjectDataCoherence coherence) 
     throws ResourceAccessException, RestClientResponseException{
         return getByQuery(catalogName, fiql, null, limit, collectionFormat, coherence);
     }
    
    /**
     * Gets a list of Object ID's based on a query
     * 
     * @param catalogName The name of the catalog
     * @param fiql the fiql query
     * @param sortString Formatted sort string PropName DESC,PropName ASC
     * @param limit The maximum number of record ids
     * @param collectionFormat The format you want your collection to be represented (this method is not ready for anything other than legacy)
     * @param coherence The desired data coherence
     * @return A list of the IDS
     * @throws ResourceAccessException On any IO type Exception
     * @throws RestClientResponseException On any response code other than 200, 201, or 404
     */
    public List<String> getByQuery(
            String catalogName, String fiql, String sortString, int limit, 
            String collectionFormat, ObjectDataCoherence coherence) 
            throws ResourceAccessException, RestClientResponseException{
        setupCorrelationId();
        HttpEntity<String> request = new HttpEntity<>(getRequiredHeaders());
        Map<String, String> parameters = new HashMap<>();
        parameters.put("fiql", fiql);
        parameters.put("limit", String.valueOf(limit));
        parameters.put("collectionFormat", collectionFormat);
        parameters.put("coherence", coherence.getCoherenceName());
        parameters.put("sort", sortString);
        String url = baseUrl + 
                "/catalogs/" + 
                catalogName + 
                "/objects?s={fiql}&limit={limit}&collectionFormat={collectionFormat}&dataCoherence={coherence}&sort={sort}";
        
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request getByQuery({0}, {1})", 
                new Object[]{catalogName, fiql});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(url, HttpMethod.GET, request, String.class, parameters);
        
        //Only responses can be a 200 or a 404
        if (response.getStatusCode() != HttpStatus.OK){
            Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(Level.SEVERE, 
                    "Server responded with status {0} {1} for getByQuery {2}", 
                    new Object[]{response.getStatusCode().name(), response.getStatusCode().getReasonPhrase(), fiql});
        }
        
        String responseString = response.getBody();
        List<String> ids;
        
        if (!StringUtils.hasText(responseString) || responseString.equals("[]")){
            ids = new ArrayList<>(); 
        }else{
            String noBracketsSpacesOrQuotes = responseString.replace("[", "").replace("]", "").replace(" ", "").replace("\"", "");
            ids = Arrays.asList(noBracketsSpacesOrQuotes.split(","));
        }
        return ids;

    }
    
    /**
     * Creates a new Object in the object catalog.
     * 
     * @param catalogName The name of the catalog to create the object in
     * @param object The object to create
     * @return An object describing the result of the operation
     * @throws JsonProcessingException If an error occurs reading the response to JSON
     * @throws RestClientResponseException If there is a response other than a 201
     * @throws ResourceAccessException if there is any I/O exception while communicating with the server
     */
    public CatalogIdentity create(String catalogName, ObjectCatalogRecordInput object) 
            throws JsonProcessingException, RestClientResponseException, ResourceAccessException{
        return create(catalogName, object, null);
    }
    
    /**
     * Creates a new Object in the object catalog.
     * 
     * @param catalogName The name of the catalog to create the object in
     * @param object The object to create
     * @param contentMD5 The MD5 checksum of the object
     * @return An object describing the result of the operation
     * @throws JsonProcessingException If an error occurs reading the response to JSON
     * @throws RestClientResponseException If there is a response other than a 201
     * @throws ResourceAccessException if there is any I/O exception while communicating with the server
     */
    public CatalogIdentity create(String catalogName, ObjectCatalogRecordInput object, String contentMD5) 
            throws JsonProcessingException, RestClientResponseException, ResourceAccessException{
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        ThreadContext.put("subRequestId", headers.getFirst("X-Request-ID"));
        Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request create({0})", 
                new Object[]{catalogName});
        ThreadContext.remove("subRequestId");
        
        if (object.getContentType() != null && !object.getContentType().isEmpty()){
            headers.add(HttpHeaders.CONTENT_TYPE, object.getContentType());
        }
        if (contentMD5 != null && !contentMD5.isEmpty()){
            headers.add("content-md5", contentMD5);
        }
        if (object.getContentEncoding() != null && !object.getContentEncoding().isEmpty()){
            headers.add(HttpHeaders.CONTENT_ENCODING, object.getContentEncoding());
        }
        if (object.getRealm() != null && !object.getRealm().isEmpty()){
            headers.add("Realm", object.getRealm());
        }
        if (!object.getTags().isEmpty()){
            headers.add("Tags", String.join(",", object.getTags()));
        }
        if (object.getMetadataCatalogIdentifiers() != null && !object.getMetadataCatalogIdentifiers().isEmpty()){
            headers.add("Metadata-Catalog-Identifiers", object.getMetadataCatalogIdentifiers().stream().collect(Collectors.joining(",")));
        }
        if (object.getObjectMetadataIdentities().size() > 0){
            headers.add("Metadata-Identities", object.getObjectMetadataIdentities().toHttpHeader());
        }
       
        InputStreamResource resource = new InputStreamResource(object.getData());
        
        HttpEntity request = new HttpEntity(
                resource, 
                headers);
        
        ResponseEntity<String> response = template.postForEntity(baseUrl + "/catalogs/" + catalogName + "/objects", request, String.class);
        
        if (response.getStatusCode() == HttpStatus.CREATED){
            CatalogIdentity identity;
            if (response.getHeaders().containsKey(HttpHeaders.LOCATION)){
                identity = CatalogIdentity.fromUrl(response.getHeaders().getFirst(HttpHeaders.LOCATION));
            }else{
                identity = new CatalogIdentity(catalogName, DotNotationMap.fromJson(response.getBody()).getProperty("id"));
            }
            return identity;
        }else{
            throw new HttpClientErrorException(response.getStatusCode(), 
                    response.getStatusCode().getReasonPhrase(), response.getHeaders(), 
                    response.getBody().getBytes(), getResponseCharset(response));
        }
    }
    
    /**
     * Creates a soft copy of an object by adding a new identity to the source record.
     * 
     * @param catalogIdentifier The name of the catalog to add the copy to
     * @param sourceRecordIdentifier The source record to add the copy to
     * @param object The copy to add
     * @param contentMD5 The MD5 checksum of the copy
     * @param realm The security realm to use
     * @return An object describing the result of the operation
     * @throws JsonProcessingException If an error occurs reading the response to JSON
     * @throws RestClientResponseException If there is a response other than a 201
     * @throws ResourceAccessException if there is any I/O exception while communicating with the server
     */
    public CatalogIdentity copy(String catalogIdentifier, String sourceRecordIdentifier, 
            ObjectCatalogRecordInput object, String contentMD5, String realm) 
            throws JsonProcessingException, RestClientResponseException, ResourceAccessException{
        
        setupCorrelationId();
        HttpHeaders headers = getRequiredHeaders();
        ThreadContext.put("subRequestId", headers.getFirst("X-Request-ID"));
        Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request copy({0}, {1})", 
                new Object[]{catalogIdentifier, sourceRecordIdentifier});
        ThreadContext.remove("subRequestId");
        
        if (object.getContentType() != null && !object.getContentType().isEmpty()){
            headers.add(HttpHeaders.CONTENT_TYPE, object.getContentType());
        }
        if (contentMD5 != null && !contentMD5.isEmpty()){
            headers.add("content-md5", contentMD5);
        }
        if (object.getContentEncoding() != null && !object.getContentEncoding().isEmpty()){
            headers.add(HttpHeaders.CONTENT_ENCODING, object.getContentEncoding());
        }
        if (realm != null && !realm.isEmpty()){
            headers.add("Realm", realm);
        }
        if (object.getTags() != null && !object.getTags().isEmpty()){
            headers.add("Tags", String.join(",", object.getTags()));
        }
        if (object.getMetadataCatalogIdentifiers() != null && !object.getMetadataCatalogIdentifiers().isEmpty()){
            headers.add("Metadata-Catalog-Identifiers", object.getMetadataCatalogIdentifiers().stream().collect(Collectors.joining(",")));
        }
        if (object.getObjectMetadataIdentities() != null && object.getObjectMetadataIdentities().size() > 0){
            headers.add("Metadata-Identities", object.getObjectMetadataIdentities().toHttpHeader());
        }
        
        headers.add("source", "catalog");
        headers.add("source-record-identifier", sourceRecordIdentifier);
        
        HttpEntity request = new HttpEntity(headers);
        
        ResponseEntity<String> response = template.postForEntity(
                baseUrl + "/catalogs/" + 
                        catalogIdentifier +
                        "/objects", 
                request, String.class);
        
        if (response.getStatusCode() == HttpStatus.CREATED){
            CatalogIdentity identity;
            if (response.getHeaders().containsKey(HttpHeaders.LOCATION)){
                identity = CatalogIdentity.fromUrl(response.getHeaders().getFirst(HttpHeaders.LOCATION));
            }else{
                identity = new CatalogIdentity(catalogIdentifier, DotNotationMap.fromJson(response.getBody()).getProperty("id"));
            }
            return identity;
        }else{
            throw new HttpClientErrorException(response.getStatusCode(), 
                    response.getStatusCode().getReasonPhrase(), response.getHeaders(), 
                    response.getBody().getBytes(), getResponseCharset(response));
        }
    }
    
    public int mergePatchByQuery(String catalogIdentifier, String fiql, DotNotationMap mergePatch, boolean updateMulti) throws JsonProcessingException{
        String url = baseUrl + "/catalogs/" + catalogIdentifier + "/objects?s=" + fiql;
        if (updateMulti){
            url = url + "&updateMulti=true";
        }else{
            url = url + "&updateMulti=false";
        }
        HttpHeaders headers = getRequiredHeaders();
        headers.set("Content-Type", "application/merge-patch+json");
        
        
        HttpEntity<String> request = new HttpEntity<>(mergePatch.toJson(), headers);
        ThreadContext.put("subRequestId", request.getHeaders().getFirst("X-Request-ID"));
        Logger.getLogger(ObjectCatalogWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request mergePatch {0}", 
                new Object[]{catalogIdentifier});
        ThreadContext.remove("subRequestId");
        
        ResponseEntity<String> response = template.exchange(
                url, 
                HttpMethod.PATCH, 
                request, 
                String.class);
        
        if (response.getStatusCode() != HttpStatus.OK){
            //Throw the 404 as an exception
            throw new HttpClientErrorException(response.getStatusCode(), 
                    response.getStatusCode().getReasonPhrase(), response.getHeaders(), 
                    response.getBody().getBytes(), getResponseCharset(response));
        }
        String numUpdated = response.getHeaders().getFirst("Num-Updated");
        if (StringUtils.hasText(numUpdated)){
            return Integer.parseInt(numUpdated);
        }else{
            return -1;
        }
    }
    
    /**
     * Gets the required headers for the request
     * 
     * @return The required headers
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
     * Adds a correlationid to the Log4j2 {@link ThreadContext} if it does not already exist.
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
