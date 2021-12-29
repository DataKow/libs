package org.datakow.catalogs.object.webservice.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation used to create the Object Catalog Web Service Client beans for client use.
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(ObjectCatalogWebServiceClientConfiguration.class)
public @interface EnableObjectCatalogWebServiceClient {
    
}
