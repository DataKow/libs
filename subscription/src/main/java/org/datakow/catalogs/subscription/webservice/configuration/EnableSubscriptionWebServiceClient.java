package org.datakow.catalogs.subscription.webservice.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation used to create the necessary beans to interact with the Subscription Web Service.
 * <p>
 * Using this annotation will create the {@link org.datakow.catalogs.subscription.webservice.SubscriptionWebserviceClient} bean to use.
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(SubscriptionAutoConfiguration.class)
public @interface EnableSubscriptionWebServiceClient {
    
}
