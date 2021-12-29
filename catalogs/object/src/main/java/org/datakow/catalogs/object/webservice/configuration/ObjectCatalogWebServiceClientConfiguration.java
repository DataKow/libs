package org.datakow.catalogs.object.webservice.configuration;


import org.datakow.catalogs.object.webservice.ObjectCatalogWebserviceClient;
import java.nio.charset.Charset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration class used to create the Object Catalog Web Service client beans to use
 * 
 * @author kevin.off
 */
@Configuration
@EnableConfigurationProperties(ObjectCatalogWebServiceClientConfigurationProperties.class)
public class ObjectCatalogWebServiceClientConfiguration {
    
    @Autowired
    ObjectCatalogWebServiceClientConfigurationProperties props;
   
    /**
     * The object catalog web service client bean to use to interact with the records
     * using the object catalog web service REST interface. 
     * 
     * @return The bean
     */
    @Bean
    public ObjectCatalogWebserviceClient objectCatalogWebserviceClient(){
        return new ObjectCatalogWebserviceClient(
            objectCatalogRestTemplate(), 
            "http://" + props.getObjectCatalogWebserviceHost() + ":" + props.getObjectCatalogWebservicePort(), 
            props.getWebserviceUsername(), 
            props.getWebservicePassword());
    }
    
    /**
     * The underlying RestTemplate bean used to make requests
     * 
     * @return The bean
     */
    @Bean
    public RestTemplate objectCatalogRestTemplate(){
        StringHttpMessageConverter converter = new StringHttpMessageConverter(Charset.forName("UTF-8"));
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(props.getObjectCatalogWebserviceClientConnectTimeout());
        factory.setReadTimeout(props.getObjectCatalogWebserviceClientReadTimeout());
        factory.setBufferRequestBody(false);
        RestTemplate template = new RestTemplate(factory);
        template.getMessageConverters().add(0, converter);
        template.setErrorHandler(new CatalogWebserviceErrorHandling());
        return template;
    }
    
}
