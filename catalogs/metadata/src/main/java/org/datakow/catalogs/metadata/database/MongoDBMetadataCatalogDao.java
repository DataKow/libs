package org.datakow.catalogs.metadata.database;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.mongodb.BasicDBList;
import com.mongodb.ReadPreference;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;

import org.datakow.catalogs.metadata.database.converters.MetadataCatalogRecordReadConverter;
import org.datakow.catalogs.metadata.database.converters.MetadataCatalogRecordWriteConverter;
import org.datakow.catalogs.metadata.jsonpatch.JsonPatchException;
import org.datakow.catalogs.metadata.jsonpatch.JsonPatchOperation;
import org.datakow.catalogs.metadata.jsonpatch.JsonPatchParser;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DateUtil;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.JsonInputStreamToIterator;
import org.datakow.core.components.DatakowObjectMapper;
import org.datakow.fiql.MongoFiqlParser;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;
import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.StringUtils;

/**
 * The MongoDB DAO used to perform CRUD operations on records in the metadata catalog.
 * 
 * @author kevin.off
 */
public class MongoDBMetadataCatalogDao {

    MongoTemplate ops;
    
    ReadPreference readPreference;
    
    static MetadataCatalogRecordReadConverter readConverter = new MetadataCatalogRecordReadConverter();
    static MetadataCatalogRecordWriteConverter writeConverter = new MetadataCatalogRecordWriteConverter();
    
    protected static final Pattern NUMBER_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?$");
    
    /**
     * Initializes the Dao with a configured {@link MongoTemplate} and the {@link ReadPreference}.
     * <p>
     * The ReadPreference is only used in the aggregate function. Read preference
     * is determined by {@link MetadataDataCoherence} parameters otherwise.
     * 
     * @param ops The configured MongoTemplate to use for all activities
     * @param preference The default read preference to use unless specified
     */
    public MongoDBMetadataCatalogDao(MongoTemplate ops, ReadPreference preference){
        this.ops = ops;
        this.readPreference = preference;
    }
    
    /**
     * Retrieves a single MetadataCatalogRecord by its Storage.Record-Identifier.
     * 
     * @param collectionName The catalog to retrieve the record from
     * @param recordIdentifier The record's ID
     * @param properties Projection properties to limit the properties that are returned
     * @param coherence The desired data coherence
     * @return The record or null on an exception
     */
    public MetadataCatalogRecord getById(String collectionName, String recordIdentifier, List<String> properties, MetadataDataCoherence coherence){
        
        if (!StringUtils.hasText(collectionName)){
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        if (!StringUtils.hasText(recordIdentifier)){
            throw new IllegalArgumentException("Record Identifier cannot be null");
        }
        
        Document q = new Document("Storage.Record-Identifier", recordIdentifier);
        ReadPreference preference = mapDataCoherence(coherence);
        Document projDocument = null;
        if (properties != null && properties.size() > 0){
            projDocument = new Document();
            for (String column : properties){
                projDocument.append(column, 1);
            }
        }

        Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, 
                "About to submit getById: Catalog: {0}. Query: {1}. Data Coherence: {2}", 
                new Object[]{collectionName, q, coherence});
        
        FindIterable<Document> result = ops
            .getCollection(collectionName)
            .withReadPreference(preference)
            .find(q);
            
            //.first();
        if (result.cursor().hasNext()) {
            MetadataCatalogRecord record = readConverter.convert(result.cursor().next());
            return record;
        }
        
        return null;
        
    }
    
    /**
     * Returns a MongoRecordStream cursor/iterator of the records retrieved by the given query.
     * 
     * @param collectionName The catalog to retrieve the records from
     * @param fiql The FIQL query string. Null for all
     * @param sortString A sort string formatted: property [ASC|DESC], ... null for no sort
     * @param limit An upper limit of the number of records to return. -1 for no limit
     * @param projection A comma delimited list of fully qualified properties to to include in the response. Null for all
     * @param coherence The desired data coherence to use in the query
     * @return The stream of records returned by the query
     */
    public MongoRecordStream<MetadataCatalogRecord> getByQuery(
        String collectionName, 
        String fiql, 
        String sortString, 
        int limit, 
        List<String> projection, 
        MetadataDataCoherence coherence){
        
        Document mappedQuery = getMappedQuery(fiql);
        ReadPreference preference = mapDataCoherence(coherence);
        
        Document projDocument = null;
        if (projection != null && projection.size() > 0){
            projDocument = new Document();
            for (String column : projection){
                projDocument.append(column, 1);
            }
        }
        
        Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, 
                "About to submit getByQuery: {0}. Data Coherence: {1}. Catalog: {2}. Sort: {3}. Limit: {4}", 
                new Object[]{mappedQuery, coherence, collectionName, sortString, limit});
        
        FindIterable<Document> iterator = ops
            .getCollection(collectionName)
            .withReadPreference(preference)
            .find(mappedQuery);

            
        if (projection != null && !projection.isEmpty())
        {
            iterator.projection(Projections.fields(Projections.include(projection)));
        }
        if (limit > 0){
            iterator.limit(limit);
        }
        if (StringUtils.hasText(sortString)){
            iterator.sort(getSortObject(sortString));
        }

        return new MongoRecordStream<>(ops.getConverter(), MetadataCatalogRecord.class, iterator.cursor());
    }
    
    
    /**
     * Uses the MongoDB aggregation pipeline to perform an aggregate search on data.
     * <p>
     * Aggregation is the act of grouping data and using functions to collect stats
     * on the group.
     * <p>
     * The pipeline is is constructed in the following way.
     * <p>
     * match(fiql) - sort(sortString|groupSort) - (first|last OR min|max|sum|avg|stdDevPop|stDevSamp) - sort(sortString) - limit
     * 
     * @param collectionName The catalog to perform the query on
     * @param fiql A FLQL query to narrow down the data
     * @param sortString Using sortString without groupSort will cause groupSort = sortString
     * @param limit Final limit on number of results
     * @param projection Accidently left projection parameter in this method. Do Not Use
     * @param groupBy The property to group the query by
     * @param groupSort The sort applied after the match is found but before the aggregation functions are applied.
     * @param near Applies a geoNear sphere stage to the pipeline at the end but before project
     * @param coherence The data coherence value to use for the query
     * @param groupFuncs Array of group functions to include in the query
     * @return A stream of results that represent the output of your query
     * @deprecated Use aggregate with the String or List&lt;Document&gt; pipeline
     */
    @Deprecated
    public MongoRecordStream<MetadataCatalogRecord> aggregate(
            String collectionName, String fiql, String sortString, 
            int limit, List<String> projection, String groupBy, 
            String groupSort, String near, MetadataDataCoherence coherence, List<String> groupFuncs){

        List<Document> pipeline = makeAggregationPipeline(fiql, sortString, limit, projection, groupBy, groupSort, near, groupFuncs);
        return aggregate(collectionName, pipeline, coherence);
        
    }
    
    public MongoRecordStream<MetadataCatalogRecord> aggregate(String collectionName, String pipeline, MetadataDataCoherence coherence){
        List<String> stages = Arrays.asList(pipeline.split("\\|"));
        List<Document> pipelineList = makeAggregationPipeline(stages);
        return aggregate(collectionName, pipelineList, coherence);
    }
    
    public MongoRecordStream<MetadataCatalogRecord> aggregate(String collectionName, List<Document> pipeline, MetadataDataCoherence coherence){
        MongoCollection<Document>collection = ops.getCollection(collectionName);
        ReadPreference preference = mapDataCoherence(coherence);
        
        if (pipeline == null || pipeline.isEmpty()){
            throw new IllegalArgumentException("The pipeline must not be empty");
        }
        
        Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, 
                "About to aggregate\ndb.{0}.aggregate(\n[{1}]\n)", 
                new Object[]{collectionName, pipeline.stream().map(d->d.toString()).collect(Collectors.joining(",\n"))});
        
        AggregateIterable<Document> cursor = collection.withReadPreference(preference).aggregate(pipeline);

        return new MongoRecordStream<>(ops.getConverter(), MetadataCatalogRecord.class, cursor.cursor());
    }
    
    /**
     * Performs a count of records given a catalog and a query.
     * 
     * @param collectionName The catalog to count records in
     * @param fiql The query to filter the records by
     * @param limit Limit the number of documents
     * @param coherence The data coherence for the query
     * @return The number of records counted
     */
    public long count(String collectionName, String fiql, int limit, MetadataDataCoherence coherence){
        ReadPreference preference = mapDataCoherence(coherence);
        if (limit <= 0){
            return 0L;
        }
        Document query = getMappedQuery(fiql);
        CountOptions options = new CountOptions();
        options.limit(limit);
        return ops.getCollection(collectionName).withReadPreference(preference).countDocuments(query, options);
        
    }
    
    /**
     * Performs a count of records given a catalog and a query.
     * 
     * @param collectionName The catalog to count records in
     * @param fiql The query to filter the records by
     * @param coherence The data coherence for the query
     * @return The number of records counted
     */
    public long count(String collectionName, String fiql, MetadataDataCoherence coherence){
        ReadPreference preference = mapDataCoherence(coherence);
        Document query = getMappedQuery(fiql);
        return ops.getCollection(collectionName).withReadPreference(preference).countDocuments(query);
        
    }
    
    public <T> DistinctIterable<T> distinct(String collectionName, String distinct, String fiql, MetadataDataCoherence coherence, Class<T> type){
        
        ReadPreference preference = mapDataCoherence(coherence);
        Document mappedQuery = getMappedQuery(fiql);
        DistinctIterable<T> iterator = ops.getCollection(collectionName).withReadPreference(preference).distinct(distinct, mappedQuery, type);
        return iterator;
    }
    
    /**
     * Creates a new record in a collection
     * 
     * @param collectionName The name of the collection
     * @param record The record to insert
     */
    public void create(String collectionName, MetadataCatalogRecord record) {
        Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, "About to create {0} in {1}", new Object[]{record.getStorage().getId(), collectionName});
        ops.getCollection(collectionName).insertOne(writeConverter.convert(record));
    }
    
    /**
     * Perform a bulk insert operation on a collection of records.
     * Does the bulk operation in batches of 1000 to save memory
     * 
     * @param collectionName The catalog to insert the records in
     * @param publisher The publisher of the data
     * @param defaultRealm Default realm to apply to the records
     * @param defaultTags Default tags to apply to the records
     * @param stream The JSON array input stream of records
     * @param defaultObjectIdentities Associated objects to use on ALL records if one has not been assigned to the individual record
     * @return A map of CorrelationId - Record-Identifier of the inserted records
     * @throws IOException When an error occurs reading the InputStream
     */
    public List<BulkResult> createBulk(
        String collectionName, 
        String publisher, 
        String defaultRealm, 
        List<String> defaultTags, 
        InputStream stream, 
        CatalogIdentityCollection defaultObjectIdentities) throws IOException{
        
        Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, "About to bulk create records in the {0} catalog", new Object[]{collectionName});
        
        JsonInputStreamToIterator<MetadataCatalogRecord> parser = JsonInputStreamToIterator.makeIterator(stream, MetadataCatalogRecord.class);
        
        List<InsertOneModel<Document>> bulkInsert = new ArrayList<>();
        List<BulkResult> bulkResult = new ArrayList<>();
            
        int recordIndex = 0;
        int batchSize = 1000;
        
        while(parser.hasNext()){
            
            List<BulkResult> batchResult = new ArrayList<>();
            
            //Collect records one by one by streaming the input in batches 
            //of 1000 or until they are gone
            while(parser.hasNext() && batchResult.size() < batchSize) {
                
                MetadataCatalogRecord record = parser.next();
                if (record.getStorage() == null && (record.getDocument() == null || record.getDocument().isEmpty())){
                    //If "nothing" was submitted then continue
                    continue;
                }
                record.setStorage(initializeDefaultStorage(record.getStorage(), publisher, defaultRealm, defaultTags, defaultObjectIdentities));
                batchResult.add(new BulkResult(record.getStorage().getId(), record.getCorrelationid(), recordIndex, "created"));
                //Add it to the list of records to insert in bulk
                bulkInsert.add(new InsertOneModel<Document>(writeConverter.convert(record)));

                recordIndex++;
            }
            
            //If there are any operations to execute
            if (!batchResult.isEmpty()){
                BulkOperationException ex = null;
                BulkWriteResult result = null;
                try{
                    result = ops.getCollection(collectionName).bulkWrite(bulkInsert);
                    
                }catch(BulkOperationException e){
                    ex = e;
                }
                bulkResult.addAll(translateBulkWriteResult(result, ex, batchResult));
            }
        }
        
        return bulkResult;
    }
    
    /**
     * Performs an upsert on one record that is found given the query.
     * <p>
     * If you specify a sort then findAndModify is used which can only be used
     * on a sharded collection if you specify the shard key in the query.
     * 
     * @param collectionName The catalog to upsert the record in
     * @param fiql The query to use to find the record. If not found one will be created
     * @param sort Sort order to update the first record. Only used if multi = false and will cause an error if the collection is sharded
     * @param record The record to upsert
     * @param publisher The publisher
     * @param upsert The property to upsert
     * @param multi If the query should update all records it finds or just one
     * @return The write result of the upsert operation. 
     * @throws com.fasterxml.jackson.core.JsonProcessingException If there is an error parsing the Object Identities array
     */
    public UpdateResult updateByQuery(
            String collectionName, String fiql, String sort, MetadataCatalogRecord record, 
            String publisher, boolean upsert, boolean multi) throws JsonProcessingException{
        
        if (upsert){
            Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, 
                    "About to upsert a record in the {0} catalog based on the query {1}", 
                    new Object[]{collectionName, fiql});
        }else{
            Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, 
                    "About to update a record in the {0} catalog based on the query {1}", 
                    new Object[]{collectionName, fiql});
        }
        
        Update updateStatement = makeUpdateStatement(record, publisher, upsert);
        Document updateObject = updateStatement.getUpdateObject();
        Document queryObject = getMappedQuery(fiql);
        Document sortObject = getSortObject(sort);
        
        return performUpdate(collectionName, queryObject, updateObject, sortObject, upsert, multi);
    }
    
    public UpdateResult performUpdate(String collectionName, Document queryObject, Document updateObject, Document sortObject, boolean upsert, boolean multi){
        
        if (!multi && sortObject != null && !sortObject.keySet().isEmpty()){
            //Reserved for a single update where the operation is sorted.
            //Note: this will fail for a sharded collection unless the Record-Identifier is the only shard key.
            Document existingRecord = ops.getCollection(collectionName).findOneAndUpdate(
                    queryObject, 
                    updateObject, 
                    new FindOneAndUpdateOptions()
                        .sort(sortObject)
                        .upsert(upsert)
                        .projection(new Document("Storage.Record-Identifier", 1))
            ); 
            
            BsonString upsertId = 
                upsert ? 
                    new BsonString(((Document)updateObject.get("$setOnInsert")).getString("Storage.Record-Identifier")) : 
                    null;
            
            UpdateResult result = UpdateResult.acknowledged(
                    existingRecord != null || upsertId != null ? (long)1 : (long)0, //If something came back or an upsert happened then 1 record was modified
                    existingRecord != null ? (long)1 : (long)0, //If something came back then something was updated
                    existingRecord == null || upsert ? upsertId : null //if old doesn't come back and upsert then upsertId
            );
            return result;
            
        }else{
            UpdateResult result = ops.getCollection(collectionName)
                .updateMany(
                    queryObject, 
                    updateObject, 
                    new UpdateOptions().upsert(upsert));
            return result;
        }
    }
    
    /**
     * Perform a bulk insert operation on a collection of MetadataCatalogRecord objects in an InputStream.
     * Does the bulk operation in batches of 1000 to save memory
     * 
     * @param collectionName The catalog to insert the records in
     * @param publisher The publisher of the data
     * @param defaultRealm Default realm to apply to the records
     * @param defaultTags Default tags to apply to the records
     * @param parameterizedFilter A filter used to produce the query for the update/upsert
     * @param recordStream The JSON array input stream of records
     * @param defaultObjectIdentities Associated objects to use on ALL records if one has not been assigned to the individual record
     * @param upsert true for upsert false for update
     * @param multi Flag to indicate if each update operation should modify the first record it finds or if it should modify all of the records that it finds
     * @return A map of CorrelationId - Record-Identifier of the inserted records
     * @throws IOException When an error occurs reading the InputStream
     * @throws JsonProcessingException If there is a problem parsing the JSON
     */
    public List<BulkResult> updateBulkByParameterizedFilter(
        String collectionName, 
        String publisher, 
        String defaultRealm,
        List<String> defaultTags, 
        String parameterizedFilter, 
        InputStream recordStream, 
        CatalogIdentityCollection defaultObjectIdentities, 
        boolean upsert, 
        boolean multi) throws IOException, JsonProcessingException {
        
        List<BulkResult> bulkResult = new ArrayList<>();
        
        Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, "About to bulk upsert records in the {0} catalog", new Object[]{collectionName});
        
        JsonInputStreamToIterator<MetadataCatalogRecord> parser = JsonInputStreamToIterator.makeIterator(recordStream, MetadataCatalogRecord.class);
        
        int recordIndex = 0;
        int batchSize = 1000;
        while(parser.hasNext()){
            
            List<BulkResult> batchResult = new ArrayList<>();
            List<WriteModel<Document>> batchQueries = new ArrayList<>();
            //Collect records one by one by streaming the input in batches 
            //of 1000 or until they are gone
            while(parser.hasNext() && batchResult.size() < batchSize) {
                
                MetadataCatalogRecord record = parser.next();
                if (record.getStorage() == null && (record.getDocument() == null || record.getDocument().isEmpty())){
                    //If nothing was submitted then continue
                    continue;
                }
                record.setStorage(initializeDefaultStorage(record.getStorage(), publisher, defaultRealm, defaultTags, defaultObjectIdentities));
                String fiql = fillParameterizedQuery(parameterizedFilter, record);
                Update updateStatement = makeUpdateStatement(record, publisher, upsert);
                batchResult.add(new BulkResult(null, record.getCorrelationid(), recordIndex, "updated"));
                
                if (multi){
                    batchQueries.add(
                        new UpdateOneModel<Document>(
                            getMappedQuery(fiql),
                            updateStatement.getUpdateObject(),
                            new UpdateOptions()
                                .upsert(upsert)
                        )
                    );
                }else{
                    batchQueries.add(
                        new UpdateManyModel<Document>(
                            getMappedQuery(fiql), 
                            updateStatement.getUpdateObject(), 
                            new UpdateOptions()
                                .upsert(upsert)
                        )
                    );
                }
                
                recordIndex++;
            }
            
            if (!batchResult.isEmpty()){
                BulkOperationException ex = null;
                BulkWriteResult result = null;
                try{
                    result = ops.getCollection(collectionName).bulkWrite(batchQueries);
                }catch(BulkOperationException e){
                    ex = e;
                }
                bulkResult.addAll(translateBulkWriteResult(result, ex, batchResult));
            }
        }
        
        return bulkResult;
    }
    
    public UpdateResult patchOneByQuery(String collectionName, String fiql, String sortString,
            List<JsonPatchOperation> operations, String publisher, boolean upsert) throws JsonProcessingException{
        MetadataCatalogRecord record;
        try(MongoRecordStream<MetadataCatalogRecord> recordStream = getByQuery(collectionName, fiql, sortString, 1, null, MetadataDataCoherence.CONSISTENT)){
            if (!recordStream.hasNext()){
                //If nothing was found but this is an upsert then we need to make an empty storage object
                if (upsert){
                    record = new MetadataCatalogRecord();
                    record.setStorage(initializeDefaultStorage(null, publisher, null, null, null));
                }else{
                    //If this was not an upsert then it was an update with 0 results
                    return UpdateResult.acknowledged(0L, 0L, null);
                }
            }else{
                record = recordStream.next();
            }
            
            record = applyJsonPatch(record, operations);
            
            UpdateResult result = updateByQuery(collectionName, "Storage.Record-Identifier==" + record.getStorage().getId(), sortString, record, publisher, upsert, false);
            return result;
        }
    }
    
    /**
     * Performs a Json Patch operation. Records are retrieved based on the query,
     * sort, and limit. Then the merge operation is performed on the records in batches
     * of 1000. Then a bulk update/upsert operation is performed in the database on the whole
     * record. This method is expensive and the mergePatchByQuery is recommended.
     * 
     * @param collectionName The name of the collection to perform the patch on
     * @param fiql The Query used to find the records to patch.
     * @param sortString Sort used to limit the number of records
     * @param limit Limits the number of records found to perform the patch on. -1 for all
     * @param operations An ordered list of the operations to perform
     * @param publisher The publisher of this patch
     * @param upsert Whether to perform an upsert or not
     * @return A list of results or null for a non-upsert where no records were found
     * @throws JsonProcessingException If there is an error parsing any JSON along the way
     */
    public List<BulkResult> patchByQuery(
        String collectionName, 
        String fiql, 
        String sortString, 
        int limit, 
        List<JsonPatchOperation> operations, 
        String publisher, 
        boolean upsert) throws JsonProcessingException{
        
        List<BulkResult> bulkResults = new ArrayList<>();
        try(MongoRecordStream<MetadataCatalogRecord> recordStream = getByQuery(collectionName, fiql, sortString, limit, null, MetadataDataCoherence.CONSISTENT)){
            
            int recordIndex = 0;
            int batchSize = 1000;
            if (recordStream.hasNext()){
                MongoCollection<Document> bulkOps = ops.getCollection(collectionName);
                while(recordStream.hasNext()){

                    List<BulkResult> batchResult = new ArrayList<>();
                    List<WriteModel<Document>> batchQueries = new ArrayList<>();

                    while(recordStream.hasNext() && batchResult.size() < batchSize){
                        MetadataCatalogRecord origRecord = recordStream.next();
                        MetadataCatalogRecord record;
                        try{
                            record = applyJsonPatch(origRecord, operations);
                        }catch(JsonPatchException ex){
                            batchResult.add(new BulkResult(origRecord.getStorage().getId(), recordIndex, "error", ex.getMessage()));
                            recordIndex++;
                            continue;
                        }

                        Update update = makeUpdateStatement(record, publisher, upsert);

                        batchQueries.add(
                            new UpdateOneModel<>(
                                new Document("Storage.Record-Identifier", record.getStorage().getId()), 
                                update.getUpdateObject(),
                                new UpdateOptions()
                                    .upsert(upsert)
                            )
                        );

                        batchResult.add(new BulkResult(record.getStorage().getId(), recordIndex, "updated"));
                        recordIndex++;
                    }
                    if (!batchResult.isEmpty()){
                        BulkWriteResult writeResult = null;
                        BulkOperationException ex = null;
                        try{
                            writeResult = bulkOps.bulkWrite(batchQueries);
                        }catch(BulkOperationException e){
                            ex = e;
                        }
                        bulkResults.addAll(translateBulkWriteResult(writeResult, ex, batchResult));
                    }
                }
            }else if (upsert){
                //if nothing was found and this is an upsert then we must create the record.
                MetadataCatalogRecord record = new MetadataCatalogRecord();
                record.setStorage(initializeDefaultStorage(null, publisher, null, null, null));
                record = applyJsonPatch(record, operations);
                create(collectionName, record);
                bulkResults.add(new BulkResult(record.getStorage().getId(), 0, "created"));
            }
        }
        
        return bulkResults;
    }
    
    public UpdateResult mergePatchByQuery(
        String collectionName, 
        String fiql, 
        String sort,
        DotNotationMap mergePatch, 
        String publisher, 
        boolean upsert, 
        boolean multi) throws JsonProcessingException{
        
        Map<String, Object> flattened = mergePatch.flatten();
        Update updateStatement = makeUpdateStatement(null, publisher, upsert);
        for(String key : flattened.keySet()){
            Object value = flattened.get(key);
            if (!key.startsWith("Doc") && !key.startsWith("Storage")){
                key = "Doc." + key;
            }
            if (value == null){
                if (upsert)
                updateStatement.unset(key);
            }else{
                updateStatement.set(key, value);
            }
        }
        
        Document updateObject = updateStatement.getUpdateObject();
        Document queryObject = getMappedQuery(fiql);
        Document sortObject = getSortObject(sort);
        
        return performUpdate(collectionName, queryObject, updateObject, sortObject, upsert, multi);
        
    }
    
    /**
     * Deletes a record by ID.
     * 
     * @param collectionName The catalog to delete the record from
     * @param recordIdentifier The ID of the record to delete
     * @return The result of the deletion
     */
    public DeleteResult deleteById(String collectionName, String recordIdentifier) {
        return deleteByQuery(collectionName, "Storage.Record-Identifier==" + recordIdentifier);
    }
    
    /**
     * Deletes records in a catalog by a give query.
     * 
     * @param collectionName The catalog to delete records from
     * @param fiql The query to use to find records to delete
     * @return The number of records deleted
     */
    public DeleteResult deleteByQuery(String collectionName, String fiql){

        Document query = getMappedQuery(fiql);
        
        Logger.getLogger(MongoDBMetadataCatalogDao.class.getName()).log(Level.INFO, 
                "About to submit deleteByQuery in {0} with {1}", 
                new Object[]{collectionName, fiql});
        
            DeleteResult writeResult = ops.getCollection(collectionName).deleteMany(query);
        return writeResult;
        
    }
    
    /**
     * Converts the sort string to a MongoDB sort Document
     * 
     * @param sortString The string to convert
     * @return The MongoDB sort Document
     */
    protected Document getSortObject(String sortString){
        return getSortObject(sortString, null);
    }
    
    /**
     * Converts the sort string to a MongoDB sort Document
     * 
     * @param sortString The string to convert
     * @param prefix A prefix to use for all property names within the sort string
     * @return The MongoDB sort Document
     */
    protected Document getSortObject(String sortString, String prefix){
        if (!StringUtils.hasText(sortString)){
            return null;
        }
        Document sort = new Document();
        
        final String prefixString = (prefix == null || prefix.isEmpty()) ? "" : prefix + ".";
        
        List<String> sortList = Arrays.asList(sortString.split(","));

        sortList.stream().forEach((String s)->{
            String[] sortListParts = s.split(" ", 2);
            if (sortListParts.length == 1){
                sort.append(prefixString + sortListParts[0], 1);
            }else if (sortListParts.length == 2){
                sort.append(prefixString + sortListParts[0], sortListParts[1].equalsIgnoreCase("DESC") ? -1 : 1);
            }
        });
        
        return sort;
    }
    
    protected Update makeUpdateStatement(MetadataCatalogRecord record, String publisher, boolean upsert) throws JsonProcessingException{
        
        Update updateStatement = new Update();
        if (record == null){
            record = new MetadataCatalogRecord();
        }
        if (record.getStorage() == null){
            record.setStorage(initializeDefaultStorage(null, publisher, null, null, null));
        }
        if (upsert){
            
            if (!StringUtils.hasText(record.getStorage().getId())){
                record.getStorage().setId(UUID.randomUUID().toString());
            }
            updateStatement.setOnInsert("Storage.Record-Identifier", record.getStorage().getId());
            updateStatement.setOnInsert("_id", record.getStorage().getId());
            
            updateStatement.setOnInsert("Storage.Publisher", record.getStorage().getPublisher());
            updateStatement.setOnInsert("Storage.Publish-Date", record.getStorage().getPublishDate());
        }
        
        updateStatement.set("Storage.Updated-By", publisher);
        updateStatement.set("Storage.Update-Date", new Date());
        
        if (record.getStorage().getTags() != null){
            updateStatement.set("Storage.Tags", record.getStorage().getTags());
        }
        if (record.getStorage().getRealm() != null){
            updateStatement.set("Storage.Realm", record.getStorage().getRealm());
        }
        if (record.getStorage().getObjectIdentities() != null){
            updateStatement.set(
                "Storage.Object-Identities", 
                DatakowObjectMapper.getDatakowObjectMapper()
                    .readValue(record.getStorage().getObjectIdentities().toJson(), new TypeReference<List<Document>>(){}));
        }
        if (record.getDocument() != null && !record.getDocument().isEmpty()){
            updateStatement.set("Doc", record.getDocument());
        }
        
        return updateStatement;
    }
    
    protected List<BulkResult> translateBulkWriteResult(BulkWriteResult writeResult, BulkOperationException ex, List<BulkResult> bulkResultList){
        
        if (ex != null){
            if (ex.getResult() !=  null){
                writeResult = ex.getResult();
            }
        }
        
        if (writeResult.wasAcknowledged()){
            //BulkWriteUpsert represents an upsert request in a bulk write operation that resulted in an insert.
            for(BulkWriteUpsert up : writeResult.getUpserts()){
                BulkResult result = bulkResultList.get(up.getIndex());
                result.setActionTaken("created");
                result.setRecordIdentifier(up.getId().asString().getValue());
            }

            for(BulkWriteInsert up : writeResult.getInserts()){
                BulkResult result = bulkResultList.get(up.getIndex());
                result.setActionTaken("created");
                result.setRecordIdentifier(up.getId().asString().getValue());
            }
        }else{
            for(BulkResult result : bulkResultList){
                result.setSuccess(false);
                result.setErrorMessage("The write was not acknowledged.");
                result.setActionTaken("error");
            }
            return bulkResultList;
        }
        
        
        if (ex != null){
            for(BulkWriteError error : ex.getErrors()){
                Integer index = error.getIndex();
                bulkResultList.get(index).setSuccess(false);
                bulkResultList.get(index).setErrorMessage(error.getMessage());
                bulkResultList.get(index).setActionTaken("error");
            }
        }
        
        return bulkResultList;
    }
    
    protected MetadataCatalogRecordStorage initializeDefaultStorage(MetadataCatalogRecordStorage storage, String publisher, String defaultRealm, 
            List<String> defaultTags, CatalogIdentityCollection defaultObjectIdentities){
        
        if (storage == null){
            storage = new MetadataCatalogRecordStorage();
        }
        storage.setId(storage.getId() == null ? UUID.randomUUID().toString() : storage.getId());
        storage.setRealm(storage.getRealm() == null ? defaultRealm : storage.getRealm());
        storage.setTags(storage.getTags() == null ? defaultTags : storage.getTags());
        storage.setObjectIdentities(storage.getObjectIdentities() == null ? defaultObjectIdentities : storage.getObjectIdentities());
        storage.setPublishDate(new Date());
        storage.setPublisher(publisher);
        
        return storage;
    }
    
    protected String fillParameterizedQuery(String parameterizedFilter, MetadataCatalogRecord record) throws JsonProcessingException{
        String[] parts = parameterizedFilter.split(";");
        List<String> queryParts = new ArrayList<>();
        DotNotationMap storageMap = null;
        for(String part : parts){
            String propertyName = part.substring(0, part.indexOf("="));
            String parameterName = part.substring(part.indexOf("{") + 1, part.lastIndexOf("}"));
            Object parameterValue;
            if (parameterName.startsWith("Storage.")){
                if (storageMap == null){
                    storageMap = DotNotationMap.fromJson(record.getStorage().toJson());
                }
                parameterValue = storageMap.getProperty(parameterName.replace("Storage.", ""));

            }else if (parameterName.startsWith("Doc.")){
                parameterValue = record.getDocument().getProperty(parameterName.replace("Doc.", ""));
            }else{
                throw new IllegalArgumentException("The record does not contain a property named " + parameterName);
            }
            if (parameterValue != null){
                if (parameterValue instanceof Date){
                    parameterValue = DateUtil.dateToUTCString((Date)parameterValue);
                }
                queryParts.add(propertyName + "==" + parameterValue);
            }else{
                throw new IllegalArgumentException("The record does not contain a property named " + parameterName);
            }
        }
        String fiql = String.join(";", queryParts);
        return fiql;
    }
    
    protected List<Document> makeAggregationPipeline(List<String> stages){
        
        List<Document> pipeline = new ArrayList<>();
        Document geoNearStage = null;
        
        for(String stageString : stages){
            
            String stageName = stageString.substring(0, stageString.indexOf("(")).trim();
            String fullStageArgument = stageString.substring(stageString.indexOf("(") + 1, stageString.lastIndexOf(")")).trim();
            switch(stageName){
                case "match":
                    Document query = getMappedQuery(fullStageArgument);
                    pipeline.add(new Document("$match", query));
                    break;
                case "limit":
                    int limit = Integer.parseInt(fullStageArgument);
                    pipeline.add(new Document("$limit", limit));
                    break;
                case "sort":
                    Document sort = getSortObject(fullStageArgument);
                    pipeline.add(new Document("$sort", sort));
                    break;
                case "group":
                    String[] groupArgs = fullStageArgument.split(",");
                    List<String> groupIds = new ArrayList<>();
                    Document funcList = new Document();
                    Document reprojection = new Document();
                    for(String groupArg : groupArgs){
                        if (!groupArg.contains("(")){
                            //Is group id param
                            groupIds.add(groupArg);
                        }else{
                            
                            //is group function
                            String functionName = groupArg.substring(0, groupArg.indexOf("(")).trim();
                            if (functionName.equals("count")){
                                funcList.append("*count", new Document("$sum", 1));
                                reprojection.append("metaDoc.count", "$*count");
                                break;
                            }
                            String functionProperty = groupArg.replace(functionName + "(", "").replace(")", "").trim();
                            String functionPropertyAlias = functionProperty.replaceAll("\\.", "*") + "*" + functionName;
                            String finalPropertyName;
                            
                            if (functionProperty.isEmpty()){
                                finalPropertyName = "metaDoc." + functionName;
                                functionProperty = "$ROOT";
                            }else{
                                finalPropertyName = functionProperty + "." + functionName;
                            }
                            
                            funcList.append(functionPropertyAlias, new Document("$" + functionName, "$" + functionProperty));
                            reprojection.append(finalPropertyName, "$" + functionPropertyAlias);
                            
                        }
                    }
                    
                    if (groupIds.isEmpty()){
                        funcList.append("_id", null);
                    }else if(groupIds.size() == 1){
                        funcList.append("_id", "$" + groupIds.get(0));
                        reprojection.append(groupIds.get(0), "$_id");
                    }else{
                        Document idList = new Document();
                        for(String idProp : groupIds){
                            idList.append(idProp.replaceAll("\\.", "*"), "$" + idProp);
                            //funcList.append("*" + idProp.replaceAll("\\.", "*") + "*first", new Document("$first", "$" + idProp));
                            reprojection.append(idProp, "$_id." + idProp.replaceAll("\\.", "*"));
                        }
                        funcList.append("_id", idList);
                    }
                    
                    pipeline.add(new Document("$group", funcList));
                    pipeline.add(new Document("$project", reprojection));
                    
                    break;
                case "project":
                    String[] projectionProperties = fullStageArgument.split(",");
                    Document project = new Document();
                    for(String projectionProperty : projectionProperties){
                        project.append(projectionProperty.trim(), 1);
                    }
                    pipeline.add(new Document("$project", project));
                    break;
                case "geoNear":
                    String[] args = fullStageArgument.split(",");
                    Document geoNearOpts = new Document();
                    geoNearOpts.append("spherical", true);
                    geoNearOpts.append("distanceField", "metaDoc.distanceFromQueryPoint");
                    for(int i = 0; i < args.length; i++){
                        String arg = args[i].trim();
                        switch (i){
                            case 0:
                                //point
                                if (!arg.contains("POINT")){
                                    throw new IllegalArgumentException("The first parameter must be a WKT POINT.");
                                }
                                String lonLatString = arg.substring(arg.indexOf("(") + 1, arg.lastIndexOf(")")).trim();
                                String[] lonLatArr = lonLatString.split(" ");
                                Document nearGeoJson = new Document("type", "Point");
                                BasicDBList nearCoords = new BasicDBList();
                                nearCoords.add(Double.parseDouble(lonLatArr[0]));
                                nearCoords.add(Double.parseDouble(lonLatArr[1]));
                                nearGeoJson.append("coordinates", nearCoords);
                                geoNearOpts.append("near", nearGeoJson);
                                break;
                            case 1:
                                //min
                                if (!arg.isEmpty() && !arg.equals("null")){
                                    Double min = Double.parseDouble(arg);
                                    if (min > 0){
                                        geoNearOpts.append("minDistance", min);
                                    }
                                }
                                break;
                            case 2:
                                //max
                                if (!arg.isEmpty() && !arg.equals("null")){
                                    Double max = Double.parseDouble(arg);
                                    if (max > 0){
                                        geoNearOpts.append("maxDistance", max);
                                    }
                                }
                                break;
                            case 3:
                                //num
                                if (!arg.isEmpty() && !arg.equals("null")){
                                    Integer num = Integer.parseInt(arg);
                                    if (num > 0){
                                        geoNearOpts.append("num", num);
                                    }
                                }
                                break;
                            case 4:
                                //query
                                if (!arg.isEmpty()){
                                    Document nearQuery = getMappedQuery(arg);
                                    geoNearOpts.append("query", nearQuery);
                                }
                                break;
                            case 5:
                                //geomField
                                if (!arg.isEmpty()){
                                    geoNearOpts.append("includeLocs", arg);
                                }
                                break;
                        }
                    }
                    geoNearStage = new Document("$geoNear", geoNearOpts);
                    break;
                default:
                    throw new IllegalArgumentException("The aggregation stage " + stageName + " is not supported");
            }
        }
        if (geoNearStage != null){
            pipeline.add(0, geoNearStage);
        }
        return pipeline;
    }
    
    @Deprecated
    protected List<Document> makeAggregationPipeline(
            String fiql, String sortString, int limit, List<String> projection, 
            String groupBy, String groupSort, String near, List<String> groupFuncs){
        
        List<Document> pipeline = new ArrayList<>();
        
        Document matchObj;
        Document groupSortObj;
        Document limitObj;
        Document sortObj = null;
        Document projDocument;
        
        Document nearObj;
        
        if (StringUtils.hasText(near)){
            nearObj = new Document();
            
            String[] pointsArr = near.replaceAll(" +", "").replace("(", "").replace(")", "").split(",");
            Document nearPoint = new Document("type", "Point");
            BasicDBList nearCoords = new BasicDBList();
            nearCoords.add(Double.parseDouble(pointsArr[0]));
            nearCoords.add(Double.parseDouble(pointsArr[1]));
            nearPoint.append("coordinates", nearCoords);
            
            nearObj.append("near", nearPoint);
            nearObj.append("maxDistance", Double.parseDouble(pointsArr[2]));
            nearObj.append("spherical", true);
            if (limit > 0){
                nearObj.append("num", limit);
            }
            if (StringUtils.hasText(fiql)){
                Document mappedQuery = getMappedQuery(fiql);
                nearObj.append("query", mappedQuery);
            }
            nearObj.append("distanceField", "metaDoc.distanceFromQueryPoint");
            pipeline.add(new Document("$geoNear", nearObj));
        }
        
        if (!StringUtils.hasText(near) && StringUtils.hasText(fiql)){
            Document mappedQuery = getMappedQuery(fiql);
            matchObj = new Document("$match", mappedQuery);
            pipeline.add(matchObj);
        }
        
        if (groupSort != null && !groupSort.isEmpty()){
            groupSortObj = getSortObject(groupSort);
            pipeline.add(new Document("$sort", groupSortObj));
        }else if (sortString != null && !sortString.isEmpty()){
            groupSortObj = getSortObject(sortString);
            sortObj = getSortObject(sortString);
            pipeline.add(new Document("$sort", groupSortObj));
        }
        
        if (groupFuncs != null && !groupFuncs.isEmpty() && StringUtils.hasText(groupBy)){
            if (groupFuncs.size() == 1 && (groupFuncs.get(0).equals("first") || groupFuncs.get(0).equals("last"))){
                Document groupFuncObj = new Document("$group", 
                                                  new Document("_id", "$" + groupBy).append("record", 
                                                          new Document("$" + groupFuncs.get(0), "$$ROOT")));
                pipeline.add(groupFuncObj);

                Document groupProjectList = new Document();
                groupProjectList.append("_id", "$record._id");
                groupProjectList.append("Storage", "$record.Storage");
                groupProjectList.append("Doc", "$record.Doc");
                pipeline.add(new Document("$project", groupProjectList));
            }else{
                Document groupFuncObj = new Document();
                Document funcList = new Document();
                Document groupProjectList = new Document();
                funcList.append("_id", "$" + groupBy);
                for(String func : groupFuncs){
                    String functionName = func.substring(0, func.indexOf("("));
                    String functionProperty = func.replace(functionName + "(", "").replace(")", "");
                    String functionPropertyAlias = functionProperty.replaceAll("\\.", "*") + "*" + functionName;

                    funcList.append(functionPropertyAlias, 
                            new Document("$" + functionName, "$" + functionProperty));

                    groupProjectList.append(functionProperty + "." + functionName, "$" + functionPropertyAlias);
                }
                groupProjectList.append(groupBy, "$_id");
                groupFuncObj.append("$group", funcList);
                pipeline.add(groupFuncObj);
                pipeline.add(new Document("$project", groupProjectList));
            }
        }
        
        if (sortObj != null && sortString != null && !sortString.isEmpty()){
            sortObj = getSortObject(sortString);
            pipeline.add(new Document("$sort", sortObj));
        }
        
        if (limit > 0){
            limitObj = new Document("$limit", limit);
            pipeline.add(limitObj);
        }
        
        if (projection != null && projection.size() > 0){
            projDocument = new Document();
            for (String column : projection){
                projDocument.append(column, 1);
            }
            pipeline.add(new Document("$project", projDocument));
        }
        return pipeline;
    }
    
    protected Document getMappedQuery(String fiql){
        Query query = makeQuery(fiql);
        QueryMapper mapper = new QueryMapper(ops.getConverter());
        Document mappedQuery = mapper.getMappedObject(query.getQueryObject(), Optional.empty());
        return mappedQuery;
    }
    
    protected Query makeQuery(String fiql){
        Query query;
        if (StringUtils.hasText(fiql)){
            MongoFiqlParser parser = new MongoFiqlParser();
            Criteria criteria = parser.parse(fiql);
            query = new Query(criteria);
        }else{
            query = new Query();
        }
        return query;
    }
    
    ReadPreference mapDataCoherence(MetadataDataCoherence coherence){
        ReadPreference preference;
        if (coherence == MetadataDataCoherence.CONSISTENT){
            preference = ReadPreference.primary();
        }else{
            preference = ReadPreference.secondaryPreferred();
        }
        return preference;
    }
    
    public MongoTemplate getMongoTemplate(){
        return this.ops;
    }
    
    protected MetadataCatalogRecord applyJsonPatch(MetadataCatalogRecord record, List<JsonPatchOperation> operations) throws JsonProcessingException{
        
        DotNotationMap recordMap = DotNotationMap.fromJson(record.toJson());
        boolean testSuccessful = JsonPatchParser.applyPatch(operations, recordMap);

        if (testSuccessful){
            MetadataCatalogRecordStorage storage = MetadataCatalogRecordStorage.fromMap(recordMap.getProperty("Storage"));
            record.setStorage(storage);
            record.setDocument(recordMap.getProperty("Doc"));
            return record;
        }else{
            throw new JsonPatchException("One or more of your JSON Patch test operations failed");
        }
    }
    
}
