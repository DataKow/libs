package org.datakow.catalogs.metadata.database.configuration;



import org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogDao;
import org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogManagementDao;
import org.datakow.catalogs.metadata.database.converters.CatalogIdentityCollectionWriteConverter;
import org.datakow.catalogs.metadata.database.converters.GeoCommandToDbObjectConverter;
import org.datakow.catalogs.metadata.database.converters.MetadataCatalogRecordReadConverter;
import org.datakow.catalogs.metadata.database.converters.MetadataCatalogRecordWriteConverter;
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
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Configures MongoDB DAO beans to use.
 * <p>
 * Not to be used directly, instead use the {@link EnableMetadataCatalogMongoClient}
 * annotation.
 * @author kevin.off
 */
@Configuration
public class MongoMetadataCatalogClientConfiguration {
    
    @Autowired
    MongoDatabaseFactory mongoFactory;
    
    @Autowired
    MongoConfigurationProperties props;
    
    /**
     * Creates a list of {@link CustomConversions} to use when moving data to
     * and from MongoDB.
     * 
     * @return The custom converters
     */
    public MongoCustomConversions customConversions(){
        List<Converter<?, ?>> converters = new ArrayList<>();
        converters.add(new MetadataCatalogRecordReadConverter());
        converters.add(new MetadataCatalogRecordWriteConverter());
        converters.add(new GeoCommandToDbObjectConverter());
        converters.add(new CatalogIdentityCollectionWriteConverter());
        return new MongoCustomConversions(converters);
    }
    
    /**
     * Creates the converter to use in the {@link MongoTemplate} with the custom
     * converters specified in customConversions.
     * 
     * @return The converter to use
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
     * Creates the MongoTemplate bean that is used in the DATAKOW Dao's.
     * 
     * @return The MongoTemplate bean
     */
    @Bean
    public MongoTemplate mongoTemplate(){
        MongoTemplate template = new MongoTemplate(mongoFactory, mongoConverter());
        template.setWriteResultChecking(WriteResultChecking.EXCEPTION);
        return template;
    }
    
    /**
     * Creates the MongoDB Dao used to interact with metadata records
     * 
     * @return The Dao
     */
    @Bean
    public MongoDBMetadataCatalogDao metadataCatalogDao(){
        MongoDBMetadataCatalogDao dao = new MongoDBMetadataCatalogDao(mongoTemplate(), props.getMongoReadPreference());
        return dao;
    }
    
    /**
     * Creates the MongoDB Dao used to modify metadata catalogs.
     * <p>
     * CRUD operations for catalogs, indexes, data retention, and schema.
     * 
     * @return The Dao
     */
    @Bean
    public MongoDBMetadataCatalogManagementDao metadataCatalogManagementDao(){
        MongoDBMetadataCatalogManagementDao dao = new MongoDBMetadataCatalogManagementDao(mongoTemplate());
        return dao;
    }
    
}
