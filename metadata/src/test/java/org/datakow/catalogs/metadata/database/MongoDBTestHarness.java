package org.datakow.catalogs.metadata.database;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.bulk.WriteRequest;

import org.datakow.catalogs.metadata.database.configuration.MongoMetadataCatalogClientConfiguration;
import org.datakow.catalogs.metadata.database.converters.MetadataCatalogRecordWriteConverter;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.hamcrest.Matchers;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 *
 * @author kevin.off
 */
public class MongoDBTestHarness {
    
    List<String> recordIdentifiers = Arrays.asList(
            "26eaf458-837e-4477-8b5a-957c0fd9a392", 
            "9a17adea-382a-4ce1-94ec-c502ad1437ac", 
            "4594c87f-fe78-4b9f-9dee-8f1dff4cc24b", 
            "48f94dbd-9e9c-4fe9-b813-f04754bfc563", 
            "08121442-1886-483d-ba70-ae1f23ed26ef");
    
    MongoDBMetadataCatalogDao dao;
    MongoCollection<Document> collection;
    
    public MongoDBMetadataCatalogDao getMockDao(){
        return dao;
    }
    
    public List<MetadataCatalogRecord> getMockRecords(int count){
        List<MetadataCatalogRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++){
            records.add(getMockRecord(i));
        }
        return records;
    }
    
    public MetadataCatalogRecord getMockRecord(int index){
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        MetadataCatalogRecordStorage storage = new MetadataCatalogRecordStorage();
        DotNotationMap doc = new DotNotationMap();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2017, 3, 24, 12, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        
        storage.setId(recordIdentifiers.get(index));
        storage.setObjectIdentities(new CatalogIdentityCollection(new CatalogIdentity("DATAKOW_OBJECTS", recordIdentifiers.get(0))));
        storage.setPublishDate(calendar.getTime());
        storage.setPublisher("awc");
        storage.setRealm("weather");
        storage.setTags(Arrays.asList("Tag1", "Tag2"));
        
        doc.setProperty("property", "value");
        doc.setProperty("someDate", "20170624T120000Z");
        
        record.setStorage(storage);
        record.setDocument(doc);
        return record;
    }
    
    public List<BulkResult> getBulkResultUpsert(){
        List<BulkResult> results = new ArrayList<>();
        for(int i = 0; i < 4; i++){
            results.add(new BulkResult(recordIdentifiers.get(i), null, i, "created"));
        }
        results.add(new BulkResult(null, 4, "updated"));
        return results;
    }
    
    public List<BulkResult> getBulkResultCreate(){
        List<BulkResult> results = new ArrayList<>();
        for(int i = 0; i < 5; i++){
            results.add(new BulkResult(recordIdentifiers.get(i), null, i, "created"));
        }
        return results;
    }
    
    public List<BulkResult> getBulkResultUpdateOne(){
        List<BulkResult> results = new ArrayList<>();
        results.add(new BulkResult(null, null, 0, "updated"));
        return results;
    }
    
    public static String EX_MONGO_CONNECT_EXCEPTION = "MongoClientException";
    public static String EX_EXECUTION_TIMEOUT = "MongoExecutionTimeoutException";
    public static String EX_BULK_WRITE_EXCEPTION = "BulkWriteException";
    public static String EX_QUERY_EXCEPTION = "MongoQueryException";
    public static String EX_WRITE_EXCEPTION = "MongoWriteException";
    public static String EX_DUPLICATE_KEY = "DuplicateKeyException";
    public static String EX_MONGO_SOCKET_EXCEPTION = "MongoSocketException";
    
    
    
    public MongoDBTestHarness() {
        
        collection = (MongoCollection<Document>)mock(MongoCollection.class);
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(mock(MongoDatabaseFactory.class));
        MongoMappingContext mappingContextMock = mock(MongoMappingContext.class);
        MongoTemplate template = mock(MongoTemplate.class);
        
        // when(collection.find(any(Document.class))).thenAnswer(i -> {
        //     MongoCursor<Document> cur = getMockDBCursor();
        //     Document obj = cur.next();
        //     cur.close();
        //     return obj;
        // });

        when(collection.withReadPreference(any(ReadPreference.class))).thenAnswer(i -> collection);
        
        AggregateIterable<Document> aggregateIterable = (AggregateIterable<Document>)mock(AggregateIterable.class);
        List<Document> metadataCatalogRecords = new ArrayList<Integer>(Arrays.asList(0,1,2,3,4))
            .stream()
            .map(i -> new MetadataCatalogRecordWriteConverter().convert(getMockRecord(i)))
            .collect(Collectors.toList());

        when(aggregateIterable.cursor()).thenAnswer(i -> getMockDBCursor(metadataCatalogRecords));
        when(collection.aggregate(Mockito.anyListOf(Document.class))).thenReturn(aggregateIterable);
        
        when(collection.countDocuments(any(Document.class), any(CountOptions.class))).thenReturn(4L);
        

        DistinctIterable<Object> distinctIterable = (DistinctIterable<Object>)mock(DistinctIterable.class);
        when(distinctIterable.cursor())
            .thenReturn(getMockDBCursor(Arrays.asList("one", "two", "three")));
        
        when(collection.distinct(anyString(), any(Bson.class), any())).thenReturn(distinctIterable);
        
        //Non upsert
        //Update performed
        //                                                                                                                            new       upsert
        when(collection.findOneAndUpdate(any(Document.class), any(Document.class), any(FindOneAndUpdateOptions.class))).thenAnswer(i -> {
            return new Document("Storage", new Document("Record-Identifier", recordIdentifiers.get(0)));
        });
        
        //                                                                upsert    multi
        when(
            collection.updateMany(
                any(Document.class), 
                any(Document.class), 
                any(UpdateOptions.class)
            )
        ).thenAnswer(i -> {
            if (i.getArgument(2, UpdateOptions.class).isUpsert()) {
                return getUpsertWriteResult();
            }else{
                return getUpdateOneWriteResult();
            }
        });


        when(collection.deleteMany(any(Document.class))).thenAnswer(i -> getRemoveOneWriteResult());
        
        when(collection.bulkWrite(any())).thenReturn(
            BulkWriteResult.acknowledged(WriteRequest.Type.INSERT, 1, 1, new ArrayList<>(), new ArrayList<>())
        );

        FindIterable<Document> findIterable = (FindIterable<Document>)mock(FindIterable.class);
        when(findIterable.cursor()).thenAnswer(a -> getMockDBCursor(metadataCatalogRecords));
        when(collection.find(any(Bson.class))).thenReturn(findIterable);

        MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContextMock);
        converter.setCustomConversions(new MongoMetadataCatalogClientConfiguration().customConversions());
        converter.afterPropertiesSet();
        
        // when(template.getCollection(eq(EX_EXECUTION_TIMEOUT))).thenThrow(new MongoExecutionTimeoutException(0, "Operation exceeded time limit"));
        // when(template.getCollection(eq(EX_MONGO_CONNECT_EXCEPTION))).thenThrow(new MongoClientException("Mongo Connect Exception"));
        // when(template.getCollection(eq(EX_BULK_WRITE_EXCEPTION))).thenThrow(mock(MongoBulkWriteException.class));
        // when(template.getCollection(eq(EX_QUERY_EXCEPTION))).thenThrow(mock(MongoQueryException.class));
        // when(template.getCollection(eq(EX_WRITE_EXCEPTION))).thenThrow(mock(MongoWriteException.class));
        // when(template.getCollection(eq(EX_DUPLICATE_KEY))).thenThrow(mock(DuplicateKeyException.class));
        // when(template.getCollection(eq(EX_MONGO_SOCKET_EXCEPTION))).thenThrow(mock(MongoSocketException.class));
        when(template.getCollection(anyString())).thenReturn(collection);
        
        when(template.getConverter()).thenReturn(converter);

        MongoDBMetadataCatalogDao realDao = new MongoDBMetadataCatalogDao(template, ReadPreference.primary());
        dao = spy(realDao);
    }
    
    private <T> MongoCursor<T> getMockDBCursor(List<T> records){

        return new MongoCursor<T>() {
            
            int count = 0;
            int limit = 999;
            
            
            @Override
            public boolean hasNext() {
                return count < records.size() && count < limit;
            }

            @Override
            public T next() {
                return records.get(count++);
            }

            @Override
            public void close() {
            }

            @Override
            public T tryNext() {
                return next();
            }

            @Override
            public ServerCursor getServerCursor() {
                return null;
            }

            @Override
            public ServerAddress getServerAddress() {
                return null;
            }
            
        };
    }
    
    
    
    private UpdateResult getUpdateOneWriteResult(){
        return UpdateResult.acknowledged(1L, 1L, null);
    }
    
    private UpdateResult getUpdateFiveWriteResult(){
        return UpdateResult.acknowledged(5L, 5L, null);
    }
    
    private UpdateResult getUpsertWriteResult(){
        return UpdateResult.acknowledged(5L, 5L, new BsonString(recordIdentifiers.get(0)));
    }
    
    private UpdateResult getNoActionWriteResult(){
        return UpdateResult.acknowledged(0L, 0L, null);
    }
    
    private DeleteResult getRemoveOneWriteResult(){
        return DeleteResult.acknowledged(1L);
    }
    
    private BulkWriteResult getBulkWriteResult(){
        return new BulkWriteResult() {

            @Override
            public int getInsertedCount() {
                return 5;
            }

            @Override
            public int getMatchedCount() {
                return 25;
            }

            @Override
            public int getModifiedCount() {
                return 5;
            }

            @Override
            public List<BulkWriteUpsert> getUpserts() {
                List<BulkWriteUpsert> upserts = new ArrayList<>();
                for (int i = 0; i < 4; i++){
                    upserts.add(new BulkWriteUpsert(i, new BsonString(recordIdentifiers.get(i))));
                }
                return upserts;
            }

            @Override
            public boolean wasAcknowledged() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public int getDeletedCount() {
                // TODO Auto-generated method stub
                return 5;
            }

            @Override
            public List<BulkWriteInsert> getInserts() {
                List<BulkWriteInsert> upserts = new ArrayList<>();
                for (int i = 0; i < 4; i++){
                    upserts.add(new BulkWriteInsert(i, new BsonString(recordIdentifiers.get(i))));
                }
                return upserts;
            }
        };
    }
    
}
