package org.datakow.catalogs.metadata.database.configuration;

import org.datakow.configuration.mongo.EnableMongo;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Creates a MongoDB connection and creates the MongoDB DAO beans
 * for you to use.
 * Will create these two beans:
 * <p>
 * {@link org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogDao}
 * {@link org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogManagementDao}
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@EnableMongo
@Import(MongoMetadataCatalogClientConfiguration.class)
public @interface EnableMetadataCatalogMongoClient {
    
}
