package org.datakow.catalogs.subscription.webservice;



import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.catalogs.subscription.QueryStringSubscription;
import org.datakow.catalogs.subscription.webservice.configuration.SubscriptionConfigurationProperties;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.JsonInputStreamToIterator;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;




/**
 * The web service client used to interact with the Subscription Web Service using 
 * its REST interface.
 * 
 * @author kevin.off
 */
public class SubscriptionWebserviceClient {

    HttpClient client;
    
    String baseUrl;
    
    @Autowired
    SubscriptionConfigurationProperties props;

    /**
     * Creates an instance of the web service client pointing to a base URL
     * 
     * @param subscriptionWebServiceBaseUrl The base URL of the web service
     */
    public SubscriptionWebserviceClient(String subscriptionWebServiceBaseUrl) {
        RequestConfig config = RequestConfig.custom().setConnectionRequestTimeout(20000).build();
        client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        baseUrl = subscriptionWebServiceBaseUrl;
    }
    
    /**
     * Gets a subscription by its ID
     * 
     * @param subscriptionId The ID
     * @return The subscription or null if not found
     * @throws IOException If the record cannot be retrieved or converted to JSON
     * @throws JsonProcessingException If there is a JSON parsing error
     */
    public QueryStringSubscription getById(String subscriptionId) throws IOException, JsonProcessingException {

        setupCorrelationId();
        HttpGet getRequest = new HttpGet(baseUrl + "/subscriptions/" + subscriptionId);
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        getRequest.setHeaders(getRequiredHeaders());
        getRequest.addHeader(requestIdHeader);
        
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, "Sending request getById({0}) [{1}]", 
                new Object[]{subscriptionId, requestId});
        ThreadContext.remove("subRequestId");
        
        HttpResponse response;
        QueryStringSubscription record = null;
        try {
            response = client.execute(getRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                record = QueryStringSubscription.fromJson(IOUtils.toString(response.getEntity().getContent()));
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on getById({2}) [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), subscriptionId, requestId});
            }
        }finally{
            getRequest.releaseConnection();
        }
        return record;
    }

    /**
     * Gets all subscriptions
     * 
     * @return A list of all subscriptions
     * @throws IOException If there is an error communicating with the subscription web service or if there is an error parsing the record
     */
    public List<QueryStringSubscription> getAll() throws IOException{
        return getByQuery(null, -1);
    }
    
    /**
     * Gets subscriptions by the username property
     * 
     * @param username The username to search for subscriptions by
     * @return A list of matching subscriptions
     * @throws IOException If there is an error communicating with the subscription web service
     * @throws JsonProcessingException if there is an error parsing the record to JSON
     */
    public List<QueryStringSubscription> getByUsername(String username) throws IOException, JsonProcessingException{
        
        setupCorrelationId();
        HttpGet getRequest = new HttpGet(baseUrl + "/subscriptions/users/" + username);
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        getRequest.setHeaders(getRequiredHeaders());
        getRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request getByUsername({0}) [{1}]", 
                new Object[]{username, requestId});
        ThreadContext.remove("subRequestId");
        
        HttpResponse response;
        List<QueryStringSubscription> records = new ArrayList<>();
        try {
            
            response = client.execute(getRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                getRequest.releaseConnection();
                JsonInputStreamToIterator<QueryStringSubscription> iterator = JsonInputStreamToIterator.makeIterator(response.getEntity().getContent(), QueryStringSubscription.class);
                while(iterator.hasNext()){
                    records.add(iterator.next());
                }
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on getByUsername({2}) [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), username, requestId});
            }
        }finally{
            getRequest.releaseConnection();
        }
        return records;
    }
    
    /**
     * Gets subscriptions by the catalogIdentifier property
     * 
     * @param catalogIdentifier The catalogIdentifier to search for subscriptions by
     * @return A list of matching subscriptions
     * @throws IOException If there is an error communicating with the subscription web service
     * @throws JsonProcessingException if there is an error parsing the record to JSON
     */
    public List<QueryStringSubscription> getByCatalogIdentifier(String catalogIdentifier) throws IOException, JsonProcessingException{
        
        setupCorrelationId();
        HttpGet getRequest = new HttpGet(baseUrl + "/subscriptions/catalogs/" + catalogIdentifier);
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        getRequest.setHeaders(getRequiredHeaders());
        getRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request getByCatalogIdentifier({0}) [{1}]", 
                new Object[]{catalogIdentifier, requestId});
        ThreadContext.remove("subRequestId");
        
        HttpResponse response;
        List<QueryStringSubscription> records = new ArrayList<>();
        try {
            
            response = client.execute(getRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String recordsJson = IOUtils.toString(response.getEntity().getContent());
                getRequest.releaseConnection();
                DotNotationList recordsJsonArray = DotNotationList.fromJson(recordsJson);
                for(Object recordObject : recordsJsonArray){
                    records.add(QueryStringSubscription.fromJson(((DotNotationMap)recordObject).toJson()));
                }
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on getByCatalogIdentifier({2}) [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), catalogIdentifier, requestId});
            }
        }finally{
            getRequest.releaseConnection();
        }
        return records;
    }
    
    /**
     * Gets subscriptions by the endpointIdentifier property
     * 
     * @param endpointIdentifier The endpointIdentifier to search for subscriptions by
     * @return A list of matching subscriptions
     * @throws IOException If there is an error communicating with the subscription web service
     * @throws JsonProcessingException if there is an error parsing the record to JSON
     */
    public List<QueryStringSubscription> getByEndpointIdentifier(String endpointIdentifier) throws IOException, JsonProcessingException{
        
        setupCorrelationId();
        HttpGet getRequest = new HttpGet(baseUrl + "/subscriptions/endpoints/" + endpointIdentifier);
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        getRequest.setHeaders(getRequiredHeaders());
        getRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request getByEndpointIdentifier({0}) [{1}]", 
                new Object[]{endpointIdentifier, requestId});
        ThreadContext.remove("subRequestId");
        
        HttpResponse response;
        List<QueryStringSubscription> records = new ArrayList<>();
        try {
            
            response = client.execute(getRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String recordsJson = IOUtils.toString(response.getEntity().getContent());
                getRequest.releaseConnection();
                DotNotationList recordsJsonArray = DotNotationList.fromJson(recordsJson);
                for(Object recordObject : recordsJsonArray){
                    records.add(QueryStringSubscription.fromJson(((DotNotationMap)recordObject).toJson()));
                }
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on getByEndpointIdentifier({2}) [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), endpointIdentifier, requestId});
            }
        }finally{
            getRequest.releaseConnection();
        }
        return records;
    }
    
    /**
     * Gets subscriptions by the a FIQL query
     * 
     * @param fiql The fiql query
     * @param limit The maximum number of records to return or -1 for unlimited
     * @return A list of matching subscriptions
     * @throws IOException If there is an error communicating with the subscription web service
     * @throws JsonProcessingException if there is an error parsing the record to JSON
     */
    public List<QueryStringSubscription> getByQuery(String fiql, int limit) throws IOException, JsonProcessingException{
        
        setupCorrelationId();
        List<String> args = new ArrayList<>();
        if (fiql != null && !fiql.isEmpty()){
            args.add("s=" + fiql);
        }
        if (limit > 0){
            args.add("limit=" + limit);
        }
        String queryStringArgs = "";
        if (!args.isEmpty()){
            queryStringArgs = "?" + String.join("&", args);
        }
        
        HttpGet getRequest = new HttpGet(baseUrl + "/subscriptions" + queryStringArgs);
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        getRequest.setHeaders(getRequiredHeaders());
        getRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request getByQuery({0}, limit={1}) [{2}]", 
                new Object[]{fiql, limit, requestId});
        ThreadContext.remove("subRequestId");
        
        HttpResponse response;
        List<QueryStringSubscription> records = new ArrayList<>();
        try {
            response = client.execute(getRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String recordsJson = IOUtils.toString(response.getEntity().getContent());
                getRequest.releaseConnection();
                DotNotationList recordsJsonArray = DotNotationList.fromJson(recordsJson);
                for(Object recordObject : recordsJsonArray){
                    records.add(QueryStringSubscription.fromJson(((DotNotationMap)recordObject).toJson()));
                }
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on getByQuery({2}, limit={3}) [{4}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), fiql, limit, requestId});
            }
        }catch(ConnectException e){
            Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(Level.SEVERE, "Cannot connect to the subscription-ws", e);
            throw e;
        }
        catch(Exception e) {
            Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(Level.SEVERE, "Error querying the subscription-ws", e);
        }
        
        finally{
            getRequest.releaseConnection();
        }
        return records;
    }
    
    /**
     * Calls the subscription web service's subscribe endpoint.
     * <p>
     * This call will cause a subscription to be created and passed into the DATAKOW
     * system where services will begin to service your subscription. You will
     * also receive a queue which will be named q.subscriber.[SubscriptionId]
     * 
     * @param subscriptionId The subscription ID you would like to use
     * @param fiql The Subscription Query to use to filter your notifications with
     * @param endpointIdentifier If there is a specific endpoint that should be used to send you your notifications
     * @param catalogIdentifier The metadata catalog identifier that your subscription is targeting
     * @param catalogAction The Action you are interested in (created|associated)
     * @param additionalProperties If there are any additional parameters that your endpoint will need
     * @return true on success
     * @throws IOException If there is an error communicating with the web service or if there is an error parsing JSON
     */
    public boolean subscribe(
            String subscriptionId, 
            String fiql, 
            String endpointIdentifier, 
            String catalogIdentifier, 
            String catalogAction, 
            Map<String, String> additionalProperties) throws IOException{
        setupCorrelationId();
        subscriptionId = (subscriptionId == null ? "" : subscriptionId);
        fiql = (fiql == null ? "" : fiql);
        endpointIdentifier = (endpointIdentifier == null ? "" : endpointIdentifier);
        catalogIdentifier = (catalogIdentifier == null ? "" : catalogIdentifier);
        catalogAction = (catalogAction == null ? "" : catalogAction);
        
        String queryString = "?id=" + subscriptionId + 
                    "&endpointIdentifier=" + endpointIdentifier + 
                    "&catalogIdentifier=" + catalogIdentifier + 
                    "&catalogAction=" + catalogAction + 
                    "&s=" + fiql;
        
        if (additionalProperties != null && additionalProperties.size() > 0){
            String extra = additionalProperties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));
            queryString += "&" + extra;
        }
        
        HttpPost postRequest = new HttpPost(baseUrl + "/subscriptions" + queryString);
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        postRequest.setHeaders(getRequiredHeaders());
        postRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request subscribe {0} [{1}]", 
                new Object[]{queryString, requestId});
        ThreadContext.remove("subRequestId");
        
        boolean success = false;
        try {
            HttpResponse response = client.execute(postRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK || response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED) {
                success = true;
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on subscribe {2} [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), queryString, requestId});
            }
        }finally{
            postRequest.releaseConnection();
        }
        return success;
    }
    
    /**
     * Creates a DELETE request to the subscription web service.
     * <p>
     * This will cause your subscription to be deleted from the catalog. It will
     * also alert DATAKOW services that your subscription is gone. Your subscriber
     * queue will also be deleted.
     * 
     * @param subscriptionId The subscription ID
     * @return true on success
     * @throws IOException If there is an error communicating with the web service
     */
    public boolean unsubscribe(String subscriptionId) throws IOException {
           
        setupCorrelationId();
        
        HttpDelete deleteRequest = new HttpDelete(baseUrl + "/subscriptions/" + subscriptionId);
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        deleteRequest.setHeaders(getRequiredHeaders());
        deleteRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request unsubscribe({0}) [{1}]", 
                new Object[]{subscriptionId, requestId});
        ThreadContext.remove("subRequestId");
        
        boolean success = false;
        try {
            HttpResponse response = client.execute(deleteRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                success = true;
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on unsubscribe({2}) [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), subscriptionId, requestId});
            }
        }finally{
            deleteRequest.releaseConnection();
        }
        return success;
    }
    
    /**
     * Sends a POST request to the pause endpoint of a subscription.
     * <p>
     * This will cause an event to be sent to the process that is servicing your
     * subscription and will cause it to stop listening to your queue.
     * This method will not delete your queue and it will continue receiving notifications.
     * Use the resume method to continue receiving notifications.
     * 
     * @param subscriptionId The subscriptionid to pause
     * @return true on success
     * @throws IOException If there is an error communicating with the web service
     */
    public boolean pause(String subscriptionId) throws IOException{
        
        setupCorrelationId();
        
        HttpDelete pauseRequest = new HttpDelete(baseUrl + "/subscriptions/" + subscriptionId + "/pause");
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        pauseRequest.setHeaders(getRequiredHeaders());
        pauseRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request pause({0}) [{1}]", 
                new Object[]{subscriptionId, requestId});
        ThreadContext.remove("subRequestId");
        
        boolean success = false;
        try {
            HttpResponse response = client.execute(pauseRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                success = true;
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on pause({2}) [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), subscriptionId, requestId});
            }
        }finally{
            pauseRequest.releaseConnection();
        }
        return success;
    }
    
    /**
     * Sends a POST request to the resume endpoint of a subscription.
     * <p>
     * This will cause an event to be sent to the process that is servicing your
     * subscription and will cause it to resume listening to your queue.
     * Notifications that are in your queue will begin to flow to your application
     * 
     * @param subscriptionId The subscriptionid to resume
     * @return true on success
     * @throws IOException If there is an error communicating with the web service
     */
    public boolean resume(String subscriptionId) throws IOException{
        
        setupCorrelationId();
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(Level.INFO, "Sending RESUME request to: {0}/subscriptions/{1}", new Object[]{baseUrl, subscriptionId});
        
        HttpDelete resumeRequest = new HttpDelete(baseUrl + "/subscriptions/" + subscriptionId + "/resume");
        String requestId = UUID.randomUUID().toString();
        Header requestIdHeader = new BasicHeader("X-Request-ID", requestId);
        resumeRequest.setHeaders(getRequiredHeaders());
        resumeRequest.addHeader(requestIdHeader);
        ThreadContext.put("subRequestId", requestId);
        Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                Level.INFO, 
                "Sending request resume({0}) [{1}]", 
                new Object[]{subscriptionId, requestId});
        ThreadContext.remove("subRequestId");
        
        boolean success = false;
        try {
            HttpResponse response = client.execute(resumeRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                success = true;
            } else {
                Logger.getLogger(SubscriptionWebserviceClient.class.getName()).log(
                        Level.SEVERE, 
                        "The server responded with an error code ({0}) {1} on resume({2}) [{3}]", 
                        new Object[]{response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), subscriptionId, requestId});
            }
        }finally{
            resumeRequest.releaseConnection();
        }
        return success;
    }

    /**
     * Gets the headers that are required for every request.
     * 
     * @return The required headers
     */
    private Header[] getRequiredHeaders(){
        Header[] headers;
        int i = 0;
        if (props.getWebserviceUsername() != null && !props.getWebservicePassword().isEmpty()){
            Header h = new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + DatatypeConverter.printBase64Binary((props.getWebserviceUsername()+":"+props.getWebservicePassword()).getBytes()));
            headers = new Header[2];
            headers[i] = h;
            i++;
        }else{
            headers = new Header[1];
        }
        String correlationId = ThreadContext.get("correlationId");
        if (correlationId == null || correlationId.isEmpty()){
            correlationId = UUID.randomUUID().toString();
            ThreadContext.put("correlationId", correlationId);
        }
        Header correlationIdHeader = new BasicHeader("X-Correlation-ID", correlationId);
        headers[i] = correlationIdHeader;
        return headers;
    }
    
    /**
     * Puts a correlationId in the ThreadContext if one does not already exist
     */
    private void setupCorrelationId(){
        String correlationId = ThreadContext.get("correlationId");
        if (correlationId == null || correlationId.isEmpty()){
            ThreadContext.put("correlationId", correlationId);
        }
    }
    
}
