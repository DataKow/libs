package org.datakow.catalogs.metadata.database;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.MongoBulkWriteException;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.json.JSONException;

import com.mongodb.ReadPreference;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import org.datakow.catalogs.metadata.jsonpatch.JsonPatchOperation;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.IteratorToInputStream;
import org.datakow.core.components.DatakowObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 *
 * @author kevin.off
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoDBMetadataCatalogDaoTest {
    
    private MongoDBMetadataCatalogDao dao;
    private MongoDBTestHarness harness;
    
    
    private final String collectionName = "DATAKOW_CATALOG";
    private final String fiql = "Doc.Property==Value";
    private final MetadataDataCoherence coherence = MetadataDataCoherence.AVAILABLE;
    private final String sort = "Property ASC";
    private final int limit = 1;
    private final List<String> projection = Arrays.asList("Property");
    private final String publisher = "bob";
    private final String realm = "secret";
    
    private final String groupBy = "Doc.Property";
    private final String groupSort = "Doc.Property ASC";
    private final String near = "(10,10,10)";
    private final List<String> groupFuncs = Arrays.asList("min(Doc.Property)");
    
    @Before
    public void setup() {
        harness = new MongoDBTestHarness();
        dao = harness.getMockDao();
    }

    private MetadataCatalogRecord getMockRecord(int index){
        return harness.getMockRecord(index);
    }
    
    @Test
    public void testGetById() throws JsonProcessingException {
        MetadataCatalogRecord record = dao.getById(collectionName, UUID.randomUUID().toString(), null, coherence);
        assertEquals(getMockRecord(0).toJson(), record.toJson());
    }

    @Test
    public void testGetByQuery() throws JsonProcessingException {
        MongoRecordStream<MetadataCatalogRecord> records = dao.getByQuery(
                collectionName, 
                fiql, 
                sort, 
                4, 
                projection, 
                MetadataDataCoherence.CONSISTENT);
        int count = 0;
        while(records.hasNext()){
            assertEquals(getMockRecord(count).toJson(), records.next().toJson());
            count++;
        }
        assertEquals(5, count);
    }
    
    @Test
    public void testGetByGeospatialQuery() throws JsonProcessingException {
        MongoRecordStream<MetadataCatalogRecord> records = dao.getByQuery(
                collectionName, 
                "Doc.Something=near=(-92.212, 23.2214, 123456);Doc.somethingelse=intersects=\"POINT(-89.25 35.21)\"", 
                sort, 
                4, 
                projection, 
                MetadataDataCoherence.CONSISTENT);
        int count = 0;
        while(records.hasNext()){
            assertEquals(getMockRecord(count).toJson(), records.next().toJson());
            count++;
        }
        assertEquals(5, count);
    }
    
    @Test
    public void testGetByWithinQuery() throws JsonProcessingException {
        MongoRecordStream<MetadataCatalogRecord> records = dao.getByQuery(
                collectionName, 
                "Doc.Something=within=\"POLYGON(-92.212 23.2214, -91.212 23.2214, -92.212 24.2214, -92.212 23.2214)\"", 
                sort, 
                4, 
                projection, 
                MetadataDataCoherence.CONSISTENT);
        int count = 0;
        while(records.hasNext()){
            assertEquals(getMockRecord(count).toJson(), records.next().toJson());
            count++;
        }
        assertEquals(5, count);
    }

    @Test
    public void testAggregate_10args() throws JsonProcessingException, JSONException {
        MongoRecordStream<MetadataCatalogRecord> records = dao.aggregate(collectionName, fiql, sort, limit, projection, groupBy, groupSort, near, coherence, groupFuncs);
        int count = 0;
        while(records.hasNext()){
            JSONAssert.assertEquals(getMockRecord(count).toJson(), records.next().toJson(), false);
            count++;
        }
    }

    @Test
    public void testMakeAggregationPipeline() throws JSONException {
        List<Document> pipeline = dao.makeAggregationPipeline(fiql, sort, limit, projection, groupBy, groupSort, near, groupFuncs);
        String pipelineString = "[" + pipeline.stream().map(p -> p.toJson()).collect(Collectors.joining(",")) + "]";
        JSONAssert.assertEquals("[{ \"$geoNear\" : { \"near\" : { \"type\" : \"Point\" , \"coordinates\" : [ 10.0 , 10.0]} , \"maxDistance\" : 10.0 , \"spherical\" : true , \"num\" : 1 , \"query\" : { \"Doc.Property\" : \"Value\"} , \"distanceField\" : \"metaDoc.distanceFromQueryPoint\"}},{ \"$sort\" : { \"Doc.Property\" : 1}},{ \"$group\" : { \"_id\" : \"$Doc.Property\" , \"Doc*Property*min\" : { \"$min\" : \"$Doc.Property\"}}},{ \"$project\" : { \"Doc.Property.min\" : \"$Doc*Property*min\" , \"Doc.Property\" : \"$_id\"}},{ \"$limit\" : 1},{ \"$project\" : { \"Property\" : 1}}]", pipelineString, false);
    }
    
    @Test
    public void testMakeAggregationPipelineNoGroup() throws JSONException {
        List<Document> pipeline = dao.makeAggregationPipeline(fiql, sort, limit, projection, groupBy, groupSort, near, null);
        String pipelineString = "[" + pipeline.stream().map(p -> p.toJson()).collect(Collectors.joining(",")) + "]";
        JSONAssert.assertEquals("[{ \"$geoNear\" : { \"near\" : { \"type\" : \"Point\" , \"coordinates\" : [ 10.0 , 10.0]} , \"maxDistance\" : 10.0 , \"spherical\" : true , \"num\" : 1 , \"query\" : { \"Doc.Property\" : \"Value\"} , \"distanceField\" : \"metaDoc.distanceFromQueryPoint\"}},{ \"$sort\" : { \"Doc.Property\" : 1}},{ \"$limit\" : 1},{ \"$project\" : { \"Property\" : 1}}]", pipelineString,false);
    }
    
    @Test
    public void testMakeAggregationPipelineNoGroup2() throws JSONException {
        List<Document> pipeline = dao.makeAggregationPipeline(fiql, sort, limit, projection, groupBy, groupSort, near, new ArrayList<>());
        String pipelineString = "[" + pipeline.stream().map(p -> p.toJson()).collect(Collectors.joining(",")) + "]";
        JSONAssert.assertEquals("[{ \"$geoNear\" : { \"near\" : { \"type\" : \"Point\" , \"coordinates\" : [ 10.0 , 10.0]} , \"maxDistance\" : 10.0 , \"spherical\" : true , \"num\" : 1 , \"query\" : { \"Doc.Property\" : \"Value\"} , \"distanceField\" : \"metaDoc.distanceFromQueryPoint\"}},{ \"$sort\" : { \"Doc.Property\" : 1}},{ \"$limit\" : 1},{ \"$project\" : { \"Property\" : 1}}]", pipelineString, false);
    }
    
    @Test
    public void testMakeAggregationPipelineLimitNoGroup() throws JSONException {
        List<Document> pipeline = dao.makeAggregationPipeline(fiql, sort, -1, projection, groupBy, groupSort, null, null);
        String pipelineString = "[" + pipeline.stream().map(p -> p.toJson()).collect(Collectors.joining(",")) + "]";
        assertEquals("[{\"$match\": {\"Doc.Property\": \"Value\"}},{\"$sort\": {\"Doc.Property\": 1}},{\"$project\": {\"Property\": 1}}]", pipelineString);
        //JSONAssert.assertEquals("[{\"$match\" : {\"Doc.Property\" : \"Value\"}},{\"$sort\" : {\"Doc.Property\" : 1}},{\"$project\" : {\"Property\" : 1}}]".replace(" ", ""), pipelineString, false);
    }

    
    
    
    @Test
    public void testDeleteByQuery() {
        long num = dao.deleteByQuery(collectionName, fiql).getDeletedCount();
        assertEquals(1L, num);
    }

    @Test
    public void testGetSortObject() {
        Document srt = dao.getSortObject(sort, "Doc");
        assertEquals("Doc.Property", srt.keySet().iterator().next());
        assertEquals(1, srt.get("Doc.Property"));
        
        srt = dao.getSortObject("Doc.bananas ASC,Doc.Stuff DESC,Doc.things");
        assertEquals("Doc.bananas", srt.keySet().iterator().next());
        assertEquals(1, srt.get("Doc.bananas"));
        assertEquals(-1, srt.get("Doc.Stuff"));
        assertEquals(1, srt.get("Doc.things"));
    }

    @Test
    public void testUpdateByQuery() throws JsonProcessingException {
        //                                                                                                 upsert multi
        UpdateResult result =  dao.updateByQuery(collectionName, fiql, null, getMockRecord(0), publisher, false, false);
        assertEquals(1, result.getMatchedCount());
        assertEquals(1, result.getModifiedCount());
        assertEquals(null, result.getUpsertedId());
        
        result =  dao.updateByQuery(collectionName, fiql, "sort DESC", getMockRecord(0), publisher, false, false);
        assertEquals(1, result.getMatchedCount());
        assertEquals(1, result.getModifiedCount());
        assertEquals(null, result.getUpsertedId());
        
        result =  dao.updateByQuery(collectionName, fiql, null, getMockRecord(0), publisher, false, true);
        assertEquals(1, result.getMatchedCount());
        assertEquals(1, result.getModifiedCount());
        assertEquals(null, result.getUpsertedId());
        
        result =  dao.updateByQuery(collectionName, fiql, "sort DESC", getMockRecord(0), publisher, false, true);
        assertEquals(1, result.getMatchedCount());
        assertEquals(1, result.getModifiedCount());
        assertEquals(null, result.getUpsertedId());
        
        result =  dao.updateByQuery(collectionName, fiql, "sort DESC", getMockRecord(0), publisher, true, false);
        assertEquals(1, result.getMatchedCount());
        assertEquals(1, result.getModifiedCount());
        assertEquals(getMockRecord(0).getStorage().getId(), result.getUpsertedId().asString().getValue());
        
        result =  dao.updateByQuery(collectionName, fiql, null, getMockRecord(0), publisher, true, true);
        assertEquals(5, result.getMatchedCount());
        assertEquals(5, result.getModifiedCount());
        assertEquals(getMockRecord(0).getStorage().getId(), result.getUpsertedId().asString().getValue());
        
        result =  dao.updateByQuery(collectionName, fiql, "sort DESC", getMockRecord(0), publisher, true, true);
        assertEquals(5, result.getMatchedCount());
        assertEquals(5, result.getModifiedCount());
        assertEquals(getMockRecord(0).getStorage().getId(), result.getUpsertedId().asString().getValue());
        
    }

    @Test
    public void testUpdateBulkByParameterizedFilter() throws IOException{
        List<BulkResult> results;
        List<String> records = new ArrayList<>();
        for(int i = 0; i < 5; i++){
            records.add(getMockRecord(i).toJson());
        }
        try (InputStream stream = IteratorToInputStream.jsonObjectIteratorToJsonArrayInputStream(records.iterator())) {
            results = dao.updateBulkByParameterizedFilter(collectionName, publisher, realm, groupFuncs, "Storage.Record-Identifier=={Storage.Record-Identifier}", stream, null, true, false);
        }
        
        // for(int i = 0; i < 5; i++){
        //     assertEquals(getMockRecord(i).getStorage().getId(), results.get(i).getRecordIdentifier());
        //     assertEquals("created", results.get(i).getActionTaken());
        // }
        
        assertEquals(5, results.size());
        assertEquals(null, results.get(4).getRecordIdentifier());
        assertEquals("updated", results.get(4).getActionTaken());
        
    }
    
    @Test
    public void testDelete() {
        
        DeleteResult result = dao.deleteById(collectionName, getMockRecord(0).getStorage().getId());
        assertEquals(1, result.getDeletedCount());
        
    }

    @Test
    public void testCreate() {
        dao.create(collectionName, getMockRecord(0));
    }

    @Test
    public void testCreateBulk() throws Exception {
        List<BulkResult> results;
        List<String> records = new ArrayList<>();
        for(int i = 0; i < 5; i++){
            records.add(getMockRecord(i).toJson());
        }
        try (InputStream stream = IteratorToInputStream.jsonObjectIteratorToJsonArrayInputStream(records.iterator())) {
            results = dao.createBulk(collectionName, publisher, realm, null, stream, null);
        }
        
        for(int i = 0; i < 4; i++){
            assertEquals(getMockRecord(i).getStorage().getId(), results.get(i).getRecordIdentifier());
            assertEquals("created", results.get(i).getActionTaken());
        }
        
        assertEquals(5, results.size());
        assertEquals(getMockRecord(4).getStorage().getId(), results.get(4).getRecordIdentifier());
        assertEquals("created", results.get(4).getActionTaken());
    }

    @Test
    public void testPatchByQuery() throws JsonProcessingException{
        List<JsonPatchOperation> operations = new ArrayList<>();
        operations.add(JsonPatchOperation.add("/Doc.thingie", "value"));
        List<BulkResult> results = dao.patchByQuery(collectionName, fiql, sort, 5, operations, publisher, true);
        
        for(int i = 0; i < 4; i++){
            assertEquals(getMockRecord(i).getStorage().getId(), results.get(i).getRecordIdentifier());
            assertEquals("updated", results.get(i).getActionTaken());
        }
        
        assertEquals(5, results.size());
        assertEquals(getMockRecord(4).getStorage().getId(), results.get(4).getRecordIdentifier());
        assertEquals("updated", results.get(4).getActionTaken());
    }
    
    @Test
    public void testPatchByQueryNotMulti() throws JsonProcessingException{
        List<JsonPatchOperation> operations = new ArrayList<>();
        operations.add(JsonPatchOperation.add("/Doc.thingie", "value"));
        List<BulkResult> results = dao.patchByQuery(collectionName, fiql, sort, 5, operations, publisher, false);
        
        assertEquals(5, results.size());
        assertEquals(getMockRecord(0).getStorage().getId(), results.get(0).getRecordIdentifier());
        assertEquals("updated", results.get(0).getActionTaken());
        
    }
    
    @Test
    public void testPatchOneByQuery() throws JsonProcessingException{
        List<JsonPatchOperation> operations = new ArrayList<>();
        operations.add(JsonPatchOperation.add("/Doc.thingie", "value"));
        UpdateResult results = dao.patchOneByQuery(collectionName, fiql, sort, operations, publisher, false);
        
        assertEquals(1, results.getMatchedCount());
        assertEquals(null, results.getUpsertedId());
        assertEquals(1, results.getModifiedCount());
    }
    
    @Test
    public void testMergePatchByQuery() throws JsonProcessingException{
        DotNotationMap patch = new DotNotationMap();
        patch.setProperty("Doc.stuff", "things");
        patch.setProperty("Doc.otherStuff", 12345);
        UpdateResult result = dao.mergePatchByQuery(collectionName, fiql, sort, patch, publisher, true, true);
        assertEquals(5, result.getMatchedCount());
        assertEquals(5, result.getModifiedCount());
        assertEquals(getMockRecord(0).getStorage().getId(), result.getUpsertedId().asString().getValue());
    }
    
    @Test
    public void testDistinct() {
        DistinctIterable<String> rtn = dao.distinct(collectionName, "Doc.Property", fiql, coherence, String.class);
        List<String> val = new ArrayList<>();
        rtn.cursor().forEachRemaining(
            (String s) -> val.add(s)
        );

        assertEquals(Arrays.asList("one", "two", "three"), val);
    }

    @Test
    public void testCount() {
        long count = dao.count(collectionName, fiql, 4, coherence);
        assertEquals(4, count);
        
    }

    @Test
    public void testFillParameterizedQuery() throws JsonProcessingException{
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        MetadataCatalogRecordStorage storage = new MetadataCatalogRecordStorage();
        storage.setId("123456");
        DotNotationMap doc = new DotNotationMap();
        doc.setProperty("propertyOne", "1234567");
        record.setStorage(storage);
        record.setDocument(doc);
        String result = dao.fillParameterizedQuery("Doc.propertyOne=={Doc.propertyOne};Storage.Record-Identifier=={Storage.Record-Identifier}", record);
        String expected = "Doc.propertyOne==1234567;Storage.Record-Identifier==123456";
        assertEquals(expected, result);
    }
    
    @Test
    public void testMakeUpdateStatementUpsert() throws JsonProcessingException{
        Update update = dao.makeUpdateStatement(getMockRecord(0), "datakow", true);
        Document updateObj = update.getUpdateObject();
        Document setOnInsert = (Document)updateObj.get("$setOnInsert");
        Document set = (Document)updateObj.get("$set");
        
        assertEquals(2, updateObj.keySet().size());
        
        assertEquals(getMockRecord(0).getStorage().getId(), setOnInsert.get("_id"));
        assertEquals(getMockRecord(0).getStorage().getId(), setOnInsert.get("Storage.Record-Identifier"));
        assertEquals(getMockRecord(0).getStorage().getPublisher(), setOnInsert.get("Storage.Publisher"));
        assertEquals(getMockRecord(0).getStorage().getPublishDate(), setOnInsert.get("Storage.Publish-Date"));
        assertEquals(4, setOnInsert.keySet().size());
        
        assertEquals("datakow", set.get("Storage.Updated-By"));
        assertTrue(Date.class.isAssignableFrom(set.get("Storage.Update-Date").getClass()));
        assertEquals(getMockRecord(0).getStorage().getRealm(), set.get("Storage.Realm"));
        BasicDBList list = new BasicDBList();
        Document obj = DatakowObjectMapper.getDatakowObjectMapper()
                        .convertValue(getMockRecord(0).getStorage().getObjectIdentities().get(0), 
                                Document.class);
        list.add(obj);
        assertEquals(list, set.get("Storage.Object-Identities"));
        assertEquals(getMockRecord(0).getDocument(), set.get("Doc"));
        assertEquals(6, set.keySet().size());
    }
    
    @Test
    public void testMakeUpdateStatementUpsertNoStorage() throws JsonProcessingException{
        MetadataCatalogRecord record = getMockRecord(0);
        record.setStorage(null);
        Update update = dao.makeUpdateStatement(record, "datakow", true);
        Document updateObj = update.getUpdateObject();
        Document setOnInsert = (Document)updateObj.get("$setOnInsert");
        Document set = (Document)updateObj.get("$set");
        
        assertEquals(2, updateObj.keySet().size());
        
        assertTrue(setOnInsert.get("_id") != null);
        assertTrue(setOnInsert.get("Storage.Record-Identifier") != null);
        assertEquals("datakow", setOnInsert.get("Storage.Publisher"));
        assertTrue(Date.class.isAssignableFrom(setOnInsert.get("Storage.Publish-Date").getClass()));
        assertEquals(4, setOnInsert.keySet().size());
        
        assertEquals("datakow", set.get("Storage.Updated-By"));
        assertTrue(Date.class.isAssignableFrom(set.get("Storage.Update-Date").getClass()));
        assertEquals(getMockRecord(0).getDocument(), set.get("Doc"));
        assertEquals(3, set.keySet().size());
    }
    
    @Test
    public void testMakeUpdateStatementUpdate() throws JsonProcessingException{
        Update update = dao.makeUpdateStatement(getMockRecord(0), "datakow", false);
        Document updateObj = update.getUpdateObject();
        Document setOnInsert = (Document)updateObj.get("$setOnInsert");
        Document set = (Document)updateObj.get("$set");
        
        assertEquals(1, updateObj.keySet().size());
        
        assertTrue(setOnInsert == null);
        
        assertEquals("datakow", set.get("Storage.Updated-By"));
        assertTrue(Date.class.isAssignableFrom(set.get("Storage.Update-Date").getClass()));
        assertEquals(getMockRecord(0).getStorage().getRealm(), set.get("Storage.Realm"));
        BasicDBList list = new BasicDBList();
        Document obj = DatakowObjectMapper.getDatakowObjectMapper()
                        .convertValue(getMockRecord(0).getStorage().getObjectIdentities().get(0), 
                                Document.class);
        list.add(obj);
        assertEquals(list, set.get("Storage.Object-Identities"));
        assertEquals(getMockRecord(0).getDocument(), set.get("Doc"));
        assertEquals(6, set.keySet().size());
    }
    
    @Test
    public void testTranslateBulkWriteResult(){
        BulkWriteResult result = mock(BulkWriteResult.class);
        List<String> uids = new ArrayList<>();
        for(int i = 0; i < 60; i++){
            uids.add(UUID.randomUUID().toString());
        }
        when(result.getUpserts()).thenAnswer(ii -> {
                List<BulkWriteUpsert> upserts = new ArrayList<>();
                for(int i = 20; i < 30; i++){
                    BulkWriteUpsert upsert = new BulkWriteUpsert(i, new BsonString(uids.get(i)));
                    upserts.add(upsert);
                }
                return upserts;
        });
        when(result.wasAcknowledged()).thenReturn(true);
        List<BulkResult> bulkresults = new ArrayList<>();
        for(int i = 0; i < 60; i++){
            BulkResult bu = new BulkResult(uids.get(i), uids.get(i), i, "updated");
            bulkresults.add(bu);
        }
        
        MongoBulkWriteException exx = mock(MongoBulkWriteException.class);
        
        when(exx.getWriteErrors()).thenAnswer(ii -> {
            List<BulkWriteError> errors = new ArrayList<>();
            for(int i = 10; i < 25; i++){
                 errors.add(new BulkWriteError(0, "error", new BsonDocument("Detail", new BsonString("detail")), i));
            }
            return errors;
        });
        
        
        BulkOperationException ex = new BulkOperationException("error", exx);
        
        List<BulkResult> test = dao.translateBulkWriteResult(result, ex, bulkresults);
        
        for(int i = 0; i < 10; i++){
            assertEquals("updated", test.get(i).getActionTaken());
            assertEquals(uids.get(i), test.get(i).getCorrelationId());
            assertEquals(uids.get(i), test.get(i).getRecordIdentifier());
            assertEquals(i, test.get(i).getSourceIndex());
            assertEquals(null, test.get(i).getErrorMessage());
            assertEquals(true, test.get(i).getSuccess());
        }
        
        for(int i = 10; i < 25; i++){
            assertEquals("error", test.get(i).getActionTaken());
            assertEquals(uids.get(i), test.get(i).getCorrelationId());
            assertEquals(uids.get(i), test.get(i).getRecordIdentifier());
            assertEquals(i, test.get(i).getSourceIndex());
            assertEquals("error", test.get(i).getErrorMessage());
            assertEquals(false, test.get(i).getSuccess());
        }
        
        for(int i = 25; i < 30; i++){
            assertEquals("created", test.get(i).getActionTaken());
            assertEquals(uids.get(i), test.get(i).getCorrelationId());
            assertEquals(uids.get(i), test.get(i).getRecordIdentifier());
            assertEquals(i, test.get(i).getSourceIndex());
            assertEquals(null, test.get(i).getErrorMessage());
            assertEquals(true, test.get(i).getSuccess());
        }
        
        for(int i = 30; i < 60; i++){
            assertEquals("updated", test.get(i).getActionTaken());
            assertEquals(uids.get(i), test.get(i).getCorrelationId());
            assertEquals(uids.get(i), test.get(i).getRecordIdentifier());
            assertEquals(i, test.get(i).getSourceIndex());
            assertEquals(null, test.get(i).getErrorMessage());
            assertEquals(true, test.get(i).getSuccess());
        }
        
    }
    
    @Test
    public void testInitializeDefaultStorage(){
        
        MetadataCatalogRecordStorage storage = dao.initializeDefaultStorage(getMockRecord(0).getStorage(), "publisher", "realm", Arrays.asList("tag","ttt"), new CatalogIdentityCollection(new CatalogIdentity("cat", "id")));
        MetadataCatalogRecord record = getMockRecord(0);
        assertEquals("publisher", storage.getPublisher());
        assertNotEquals(record.getStorage().getPublishDate(), storage.getPublishDate());
        assertEquals(record.getStorage().getTags(), storage.getTags());
        assertEquals(record.getStorage().getObjectIdentities(), storage.getObjectIdentities());
        assertEquals(record.getStorage().getId(), storage.getId());
        assertEquals(record.getStorage().getUpdateDate(), storage.getUpdateDate());
        assertEquals(record.getStorage().getUpdatedBy(), storage.getUpdatedBy());
    }
    
    @Test
    public void testInitializeDefaultStorageNullStorage(){
        
        MetadataCatalogRecordStorage storage = dao.initializeDefaultStorage(null, "publisher", "realm", Arrays.asList("tag","ttt"), new CatalogIdentityCollection(new CatalogIdentity("cat", "id")));

        assertEquals("publisher", storage.getPublisher());
        assertEquals(Date.class, storage.getPublishDate().getClass());
        assertEquals(Arrays.asList("tag","ttt"), storage.getTags());
        assertEquals(new CatalogIdentityCollection(new CatalogIdentity("cat", "id")), storage.getObjectIdentities());
        assertNotNull(storage.getId());
        assertEquals(null, storage.getUpdateDate());
        assertEquals(null, storage.getUpdatedBy());
    }
    
    @Test
    public void testMakeQuery() {
        
        Query query = dao.makeQuery(fiql);
        assertEquals("Doc.Property", query.getQueryObject().keySet().iterator().next());
        assertEquals("Value", query.getQueryObject().get("Doc.Property"));
        
        query = dao.makeQuery(null);
        assertEquals(0, query.getQueryObject().keySet().size());
        
    }
    
    @Test
    public void testMakeAggregationPipelineWithPipelineArgument() throws JsonProcessingException{
        String pipelineString = "geoNear(POINT(-80.37667 27.49806), 0, 3000, 5)|match(Doc.Icao-Id==KFPR)|limit(5)|sort(Storage.Publish-Date DESC)|group(Doc.Product-Identifier,Doc.Wmo-Id,max(Doc.Elevation),min(Doc.Elevation),first(), count())|project(Doc,metaDoc)";
        List<String> stages = Arrays.asList(pipelineString.split("\\|"));
        
        List<Document> pipelineDbObject = dao.makeAggregationPipeline(stages);
        ObjectMapper om = DatakowObjectMapper.getDatakowObjectMapper();
        String result = om.writeValueAsString(pipelineDbObject);
        String expect = "[{\"$geoNear\":{\"spherical\":true,\"distanceField\":\"metaDoc.distanceFromQueryPoint\",\"near\":{\"type\":\"Point\",\"coordinates\":[-80.37667,27.49806]},\"maxDistance\":3000.0,\"num\":5}},{\"$match\":{\"Doc.Icao-Id\":\"KFPR\"}},{\"$limit\":5},{\"$sort\":{\"Storage.Publish-Date\":-1}},{\"$group\":{\"Doc*Elevation*max\":{\"$max\":\"$Doc.Elevation\"},\"Doc*Elevation*min\":{\"$min\":\"$Doc.Elevation\"},\"*first\":{\"$first\":\"$$ROOT\"},\"*count\":{\"$sum\":1},\"_id\":{\"Doc*Product-Identifier\":\"$Doc.Product-Identifier\",\"Doc*Wmo-Id\":\"$Doc.Wmo-Id\"}}},{\"$project\":{\"Doc.Elevation.max\":\"$Doc*Elevation*max\",\"Doc.Elevation.min\":\"$Doc*Elevation*min\",\"metaDoc.first\":\"$*first\",\"metaDoc.count\":\"$*count\",\"Doc.Product-Identifier\":\"$_id.Doc*Product-Identifier\",\"Doc.Wmo-Id\":\"$_id.Doc*Wmo-Id\"}},{\"$project\":{\"Doc\":1,\"metaDoc\":1}}]";
        assertEquals(expect, result);
    }
    
    @Test
    public void testGetMappedQuery(){
        String myFiql = "Doc.things==stuff;Doc.otherThings=gt=otherthings";
        Document obj = dao.getMappedQuery(myFiql);
        Document expected = new Document();
        List<Document> list = new ArrayList<Document>();
        list.add(new Document("Doc.things", "stuff"));
        list.add(new Document("Doc.otherThings", new Document("$gt", "otherthings")));
        expected.put("$and", list);
        assertEquals(expected, obj);
    }
    
    @Test
    public void testMapDataCoherence(){
        ReadPreference secondary = ReadPreference.secondaryPreferred();
        ReadPreference primary = ReadPreference.primary();
        ReadPreference act = dao.mapDataCoherence(MetadataDataCoherence.AVAILABLE);
        assertEquals(secondary, act);
        act = dao.mapDataCoherence(MetadataDataCoherence.CONSISTENT);
        assertEquals(primary, act);
    }
    
}
