package org.datakow.catalogs.metadata.webservice.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation used to create the Metadata Catalog Web Service Client beans for client use.
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(MetadataCatalogWebServiceClientConfiguration.class)
public @interface EnableMetadataCatalogWebServiceClient {
    
}
