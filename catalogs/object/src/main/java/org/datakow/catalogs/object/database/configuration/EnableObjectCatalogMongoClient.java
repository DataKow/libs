package org.datakow.catalogs.object.database.configuration;

import org.datakow.configuration.mongo.EnableMongo;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation used to enable a connection to MongoDB and setup the necessary
 * beans to interact with the Object Catalog on MongoDB directly.
 * <p>
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@EnableMongo
@Import(MongoObjectCatalogClientConfiguration.class)
public @interface EnableObjectCatalogMongoClient {
    
}
