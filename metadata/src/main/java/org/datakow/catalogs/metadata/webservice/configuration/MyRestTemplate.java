package org.datakow.catalogs.metadata.webservice.configuration;

import org.datakow.core.components.JsonInputStreamToIterator;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author kevin.off
 */
public class MyRestTemplate extends RestTemplate{
    
    public MyRestTemplate(){
        super();
    }
    
    public MyRestTemplate(ClientHttpRequestFactory requestFactory) {
        super(requestFactory);
    }
    
    public <T> JsonInputStreamToIterator<T> exchangeForIterator(
        URI uri, 
        HttpMethod method,
        HttpEntity<?> requestEntity, 
        Class<T> recordType, 
        boolean notFoundIsException, 
        Object ... urlVariables) throws RestClientException{

        try{
            ClientHttpResponse response = exchangeForInputStream(uri, method, requestEntity, urlVariables);
            if (notFoundIsException && response.getStatusCode() == HttpStatus.NOT_FOUND){
                //Throw the 404 as an exception
                throw new HttpClientErrorException(response.getStatusCode(), 
                        response.getStatusCode().getReasonPhrase(), response.getHeaders(), 
                        IOUtils.toByteArray(response.getBody()), getResponseCharset(response));
            }
            return JsonInputStreamToIterator.makeIterator(response.getBody(), recordType);
        }catch (IOException ex) {
                String resource = uri.toString();
                String query = uri.getRawQuery();
                resource = (query != null ? resource.substring(0, resource.indexOf(query) - 1) : resource);
                throw new ResourceAccessException("I/O error on " + method.name() +
                                " request for \"" + resource + "\": " + ex.getMessage(), ex);
        }
    }
    
    /**
     * Returns an input stream for the request. If the request results in a 404 it will return null;
     * 
     * @param uri The request url
     * @param method The http method
     * @param requestEntity The request entity
     * @param urlVariables The URI variables for the url if there are any
     * @return Response from the server or null for 404
     * @throws RestClientException If something goes wrong.
     */
    public ClientHttpResponse exchangeForInputStream(URI uri, HttpMethod method,
                HttpEntity<?> requestEntity, Object... urlVariables) throws RestClientException {

        RequestCallback requestCallback = httpEntityCallback(requestEntity, InputStreamResource.class);
        
        ClientHttpResponse response;
        try {
            ClientHttpRequest request = createRequest(uri, method);
            if (requestCallback != null) {
                    requestCallback.doWithRequest(request);
            }
            response = request.execute();
            handleResponse(uri, method, response);
        }
        catch (IOException ex) {
            String resource = uri.toString();
            String query = uri.getRawQuery();
            resource = (query != null ? resource.substring(0, resource.indexOf(query) - 1) : resource);
            throw new ResourceAccessException("I/O error on " + method.name() +
                            " request for \"" + resource + "\": " + ex.getMessage(), ex);
        }

        return response;
        
    }
    
    /**
     * Gets the charset from the response.
     * 
     * @param response The response
     * @return The charset of the response
     */
    private Charset getResponseCharset(ClientHttpResponse response){
        HttpHeaders headers = response.getHeaders();
        MediaType contentType = headers.getContentType();
        Charset charset = contentType != null ? contentType.getCharset() : null;
        return  charset;
    }
    
}
