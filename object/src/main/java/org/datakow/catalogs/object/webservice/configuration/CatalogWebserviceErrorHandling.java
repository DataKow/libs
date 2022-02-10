package org.datakow.catalogs.object.webservice.configuration;

import java.io.IOException;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.DefaultResponseErrorHandler;

/**
 * Used to change how the Spring RestTemplate decides what is an error an what is not.
 * <p>
 * Modified so a 404 does not throw an exception. All other status codes greater than 400 will.
 * @author kevin.off
 */
public class CatalogWebserviceErrorHandling extends DefaultResponseErrorHandler {

    /**
     * Override hasError to make it so 404 responses are not handled as errors.
     * 
     * @param response The HTTP response to check
     * @return true if the response is an exception
     * @throws IOException If an error occurs reading the response
     */
    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        HttpStatus statusCode = response.getStatusCode();
        return (
                statusCode != HttpStatus.NOT_FOUND && 
                    (
                        statusCode.series() == HttpStatus.Series.CLIENT_ERROR || 
                        statusCode.series() == HttpStatus.Series.SERVER_ERROR)
                    );
    }
    
    
}
