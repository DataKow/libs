package org.datakow.catalogs.object.database.configuration;



import org.datakow.catalogs.object.database.MongoDBObjectCatalogDao;
import org.datakow.configuration.mongo.MongoConfigurationProperties;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

/**
 * Configuration class used to setup the beans to interact with the Object
 * Catalog directly with MongoDb
 * 
 * @author kevin.off
 */
@Configuration
public class MongoObjectCatalogClientConfiguration {
    
    @Autowired
    MongoDatabaseFactory mongoFactory;
    
    @Autowired
    MongoConfigurationProperties props;
    
    /**
     * Returns a list of custom converters to use to convert data between java
     * and MongoDb.
     * 
     * @return The list of custom converters
     */
    public CustomConversions customConversions(){
        List<Converter<?, ?>> converters = new ArrayList<>();
        return new CustomConversions(converters);
    }
    
    /**
     * The mongo converter that the MongoTemplate uses.
     * 
     * @return The Mongo Converter
     */
    public MappingMongoConverter mongoConverter() {
        MongoMappingContext mappingContext = new MongoMappingContext();
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoFactory);
        MappingMongoConverter mongoConverter = new MappingMongoConverter(dbRefResolver, mappingContext);
        mongoConverter.setCustomConversions(customConversions());
        mongoConverter.afterPropertiesSet();
        return mongoConverter;
    }
    
    /**
     * The Underlying MongoTemplate bean that is used to interact with mongo.
     * <p>
     * Not to be used directly
     * <p>
     * Used to access object catalog files collection
     * 
     * @return The underlying MongoTemplate bean
     */
    @Bean
    public MongoTemplate mongoTemplate(){
        MongoTemplate template = new MongoTemplate(mongoFactory);
        template.setWriteResultChecking(WriteResultChecking.EXCEPTION);
        return template;
    }
    
    /**
     * The underlying GridFsTemplate used to interact directly with mongo.
     * <p>
     * Not to be used directly
     * <p>
     * Used to access the object catalog as a GridFS file store
     * 
     * @return The underlying GridFsTemplate
     */
    @Bean
    public GridFsTemplate gridFsTemplate(){
        return new GridFsTemplate(mongoFactory, mongoConverter(), props.getObjectCatalogCollectionName());
    }
    
    /**
     * The DAO bean to interact with the Object Catalog
     * 
     * @return the Mongo Object Catalog DAO
     */
    @Bean
    public MongoDBObjectCatalogDao objectCatalogDao(){
        MongoDBObjectCatalogDao dao = new MongoDBObjectCatalogDao(mongoTemplate(), gridFsTemplate(), props, mongoFactory);
        return dao;
    }
    
}
