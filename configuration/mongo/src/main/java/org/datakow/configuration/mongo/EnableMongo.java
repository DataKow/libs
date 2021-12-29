package org.datakow.configuration.mongo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Imports the {@link MongoCoreAutoConfiguration} to create a connection
 * to MongoDB. It uses the properties in {@link MongoConfigurationProperties}
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(MongoCoreAutoConfiguration.class)
public @interface EnableMongo {
    
}
