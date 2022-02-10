package org.datakow.catalogs.metadata.webservice.configuration;

import org.datakow.catalogs.metadata.webservice.MetadataCatalogManagementWebserviceClient;
import org.datakow.catalogs.metadata.webservice.MetadataCatalogWebserviceClient;
import java.nio.charset.Charset;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.datakow.catalogs.metadata.CatalogRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.cloud.context.config.annotation.RefreshScope;

/**
 * Configuration class that creates beans for clients to use to access the 
 * Metadata Catalog Web Service using its REST interface. 
 * <p>
 * Creates the following beans: {@link MetadataCatalogWebserviceClient} and {@link MetadataCatalogManagementWebserviceClient} and {@link CatalogRegistry}
 * @author kevin.off
 */
@EnableConfigurationProperties(MetadataCatalogWebServiceClientConfigurationProperties.class)
public class MetadataCatalogWebServiceClientConfiguration {

    /**
     * Creates the bean that clients should use to interact with the metadata catalog via the REST interface.
     * 
     * @return The webservice bean
     */
    @Bean
    @RefreshScope
    public MetadataCatalogWebserviceClient metadataCatalogWebserviceClient(
        MyRestTemplate template,
        MetadataCatalogWebServiceClientConfigurationProperties props){
            
        return new MetadataCatalogWebserviceClient(
                template, 
                "http://" + props.getMetadataCatalogWebserviceHost() + ":" + props.getMetadataCatalogWebservicePort(), 
                props.getWebserviceUsername(), 
                props.getWebservicePassword());
    }
    
    /**
     * Creates a management bean to interact with the catalogs, indexes, schema...etc for each catalog.
     * 
     * @return The management bean
     */
    @Bean
    @RefreshScope
    public MetadataCatalogManagementWebserviceClient metadataCatalogManagementWebserviceClient(
        MyRestTemplate metadataCatalogRestTemplate,
        MetadataCatalogWebServiceClientConfigurationProperties props){
        MetadataCatalogManagementWebserviceClient client = new MetadataCatalogManagementWebserviceClient(
            metadataCatalogRestTemplate, 
            "http://" + props.getMetadataCatalogWebserviceHost() + ":" + props.getMetadataCatalogWebservicePort(), 
            props.getWebserviceUsername(), 
            props.getWebservicePassword());
        return client;
    }
    
    /**
     * Creates the RestTemplate bean that is the underlying Http connection to the web service.
     * 
     * @return The RestTemplate bean
     */
    @Bean
    @RefreshScope
    public MyRestTemplate metadataCatalogRestTemplate(MetadataCatalogWebServiceClientConfigurationProperties props){
        StringHttpMessageConverter converter = new StringHttpMessageConverter(Charset.forName("UTF-8"));
        HttpClient client;
        if (props.getMetadataCatalogWebserviceClientMaxTotalConnections() > 0
                && props.getMetadataCatalogWebserviceClientMaxTotalConnectionsPerRoute() > 0){
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            cm.setMaxTotal(props.getMetadataCatalogWebserviceClientMaxTotalConnections());
            cm.setDefaultMaxPerRoute(props.getMetadataCatalogWebserviceClientMaxTotalConnectionsPerRoute());
            client = HttpClients.custom().setConnectionManager(cm).build();
        }else{
            client = HttpClients.createDefault();
        }
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(client);
        factory.setConnectTimeout(props.getMetadataCatalogWebserviceClientConnectTimeout());
        factory.setReadTimeout(props.getMetadataCatalogWebserviceClientReadTimeout());
        factory.setBufferRequestBody(false);
        MyRestTemplate template = new MyRestTemplate(factory);
        template.getMessageConverters().add(0, converter);
        template.setErrorHandler(new CatalogWebserviceErrorHandling());
        return template;
    }
    
    /**
     * Creates a CatalogRegistry bean that can be used to cache catalog information for all catalogs.
     * <p>
     * This is to be used with the configuration properties catalogRegistryCacheTimeInMinutes and catalogRegistryIncludeIndexes.
     * 
     * @return The CatalogRegistry bean
     */
    @Bean
    @RefreshScope
    public CatalogRegistry catalogRegistry(
        MetadataCatalogManagementWebserviceClient client,
        MetadataCatalogWebServiceClientConfigurationProperties props){

        return new CatalogRegistry(client, props.getCatalogRegistryCacheTimeInMinutes());
    }
    
}
