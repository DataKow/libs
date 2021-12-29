package org.datakow.catalogs.metadata.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.catalogs.metadata.database.MetadataDataCoherence;
import org.datakow.catalogs.metadata.jsonpatch.JsonPatchOperation;
import org.datakow.catalogs.metadata.webservice.configuration.MyRestTemplate;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.CloseableIterator;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.JsonInputStreamToIterator;
import org.datakow.core.components.DatakowObjectMapper;
import java.net.URI;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Before;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

/**
 *
 * @author kevin.off
 */
public class MetadataCatalogWebserviceClientTest {
    
    private MockRestServiceServer server;
    private MyRestTemplate template;
    MetadataCatalogWebserviceClient client;
    
    public long dateEpoch = 1499102360000L;
    
    String catalogIdentifier = "DATAKOW_CATALOG";
    String fiql = "Doc.Property==Value;Doc.other=gt=\"My Name\"";
    String sort = "Doc.Property DESC";
    int limit = 10;
    String groupBy = "Doc.Property";
    String distinct = "Doc.Property";
    List<String> groupFunctions = Arrays.asList("first");
    String groupSort = "Doc.Property";
    String geoNear = "(10,10,10)";
    List<String> properties = Arrays.asList("Doc.Property1", "Doc.Property2");
    MetadataDataCoherence coherence = MetadataDataCoherence.CONSISTENT;
    String recordIdentifier = UUID.randomUUID().toString();
    String recordIdentifier2 = UUID.randomUUID().toString();
    
    public MetadataCatalogWebserviceClientTest() {
        
    }
    
    @Before
    public void setUp(){
        template = new MyRestTemplate();
        client = new MetadataCatalogWebserviceClient(template, "http://datakow.com", "datakow", "datakow");
    }

    @Test
    public void testGetById() throws Exception {
        
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withDataCoherence(coherence)
                .withProjectionProperties("Doc.stuff")
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier);
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(getMockRecordJson(), MediaType.APPLICATION_JSON));
        
        MetadataCatalogRecord record = client.getById(catalogIdentifier, recordIdentifier, Arrays.asList("Doc.stuff"), MetadataDataCoherence.CONSISTENT);
        
        assertEquals(getMockRecord().toJson(), record.toJson());
        
        
    }
    
    @Test
    public void testGetByQueryAndStream() throws Exception {
        
        MetadataCatalogWebserviceRequest request = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withSort(sort)
                .withLimit(limit)
                .withGroupBy(groupBy)
                .withGroupFunctions(groupFunctions)
                .withGroupSort(groupSort)
                .withNear(geoNear)
                .withProjectionProperties(properties)
                .withProperty("dataCoherence", coherence);

        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(request.toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records")))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(getMockRecordArrayJson(), MediaType.APPLICATION_JSON_UTF8));
        
        try (CloseableIterator<MetadataCatalogRecord> records = client.getByQueryAndStream(catalogIdentifier, request)) {
            assertEquals(records.hasNext(), true);
            assertEquals(getMockRecord().toJson(), records.next().toJson());
        }
        
    }

    @Test
    public void testDistinct() throws Exception {
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withDistinct(distinct)
                .withDataCoherence(coherence)
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/distinct");
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(DatakowObjectMapper.getObjectMapper().writeValueAsString(Arrays.asList("abc", 123)), MediaType.APPLICATION_JSON_UTF8));
        
        JsonInputStreamToIterator records = client.distinct(catalogIdentifier, distinct, fiql, coherence);
        
        assertEquals("abc", records.next());
        assertEquals(123, records.next());
        assertEquals(false, records.hasNext());
    }

    @Test
    public void testCount() throws Exception {
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withLimit(limit)
                .withDataCoherence(coherence)
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/count");
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"Num-Records\":2}", MediaType.APPLICATION_JSON_UTF8));
        
        int records = client.count(catalogIdentifier, fiql, limit, coherence);
        
        assertEquals(2, records);
    }
    
    @Test
    public void testCreate() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records");
        
        HttpHeaders headers = new HttpHeaders();
        headers.add("Location", "catalogs/" + catalogIdentifier + "/records/" + getMockRecord().getStorage().getId());
        headers.setContentType(MediaType.APPLICATION_JSON);
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().string(getMockRecord().getDocument().toJson()))
                .andExpect(header("Realm", "realm"))
                .andExpect(header("Object-Identities", getMockRecord().getStorage().getObjectIdentities().toHttpHeader()))
                .andExpect(header("Tags", "One,two"))
                .andRespond(
                        withStatus(HttpStatus.CREATED)
                                .body("{\"id\":\"" + getMockRecord().getStorage().getId() + "\"}")
                                .headers(headers));
                
        CatalogIdentity identity = client.create(
                catalogIdentifier, 
                getMockRecord().getStorage().getId(), 
                "realm", 
                Arrays.asList("One", "two"), 
                getMockRecord().getDocument(),
                getMockRecord().getStorage().getObjectIdentities());
        
        assertEquals(new CatalogIdentity(catalogIdentifier, getMockRecord().getStorage().getId()), identity);
    }
    
    @Test
    public void testUpdateOneById() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records/" + getMockRecord().getStorage().getId());
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(getMockRecord().getDocument().toJson()))
                .andExpect(header("Realm", "realm"))
                .andExpect(header("Object-Identities", getMockRecord().getStorage().getObjectIdentities().toHttpHeader()))
                .andExpect(header("Tags", "One,two"))
                .andRespond(
                        withStatus(HttpStatus.OK)
                                .headers(headers));
                
        client.updateOneById(catalogIdentifier, recordIdentifier, "realm", Arrays.asList("One", "two"), getMockRecord().getDocument(), getMockRecord().getStorage().getObjectIdentities());
    }
    
    @Test
    public void testUpdateOneByQuery() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withSort(sort)
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records");
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(getMockRecord().getDocument().toJson()))
                .andExpect(header("Realm", "realm"))
                .andExpect(header("Object-Identities", getMockRecord().getStorage().getObjectIdentities().toHttpHeader()))
                .andExpect(header("Tags", "One,two"))
                .andRespond(
                        withStatus(HttpStatus.OK)
                                .headers(headers));
                
        client.updateOneByQuery(catalogIdentifier, fiql, sort, "realm", Arrays.asList("One", "two"), getMockRecord().getDocument(), getMockRecord().getStorage().getObjectIdentities());
    }
    
    @Test
    public void testUpdateBulkByParameterizedFilter() throws Exception{
        URI expectUrl = MetadataCatalogWebserviceRequest.builder()
                .withFilter("Storage.Record-Identifier=={Storage.Record-Identifier}")
                .withMulti(false)
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records");
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(expectUrl))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(getMockRecordArrayJson()))
                .andExpect(header("Realm", "realm"))
                .andExpect(header("Object-Identities", getMockRecord().getStorage().getObjectIdentities().toHttpHeader()))
                .andExpect(header("Tags", "One,two"))
                .andRespond(
                        withStatus(HttpStatus.OK)
                                .headers(headers)
                                .body(getBulkUpdateResponseArrayJson()));
                
        JsonInputStreamToIterator<BulkResult> results = client.updateBulkByParameterizedFilter(catalogIdentifier, 
                "Storage.Record-Identifier=={Storage.Record-Identifier}", "realm", 
                Arrays.asList("One", "two"), IOUtils.toInputStream(getMockRecordArrayJson()), false, 
                getMockRecord().getStorage().getObjectIdentities());
        List<BulkResult> expected = getBulkUpdateResponseArray();
        assertEquals(expected.get(0), results.next());
        assertEquals(expected.get(1), results.next());
        assertTrue(!results.hasNext());
    }
    
    
    
    @Test
    public void testCreateBulk() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records");
        
        DotNotationList<MetadataCatalogRecord> records =  new DotNotationList<>();
        records.add(getMockRecord());
        records.add(getMockRecord());
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header("Operation-Type", "bulk"))
                .andExpect(header("Tags", "tag1,tag2"))
                .andExpect(header("Object-Identities", getMockRecord().getStorage().getObjectIdentities().toHttpHeader()))
                .andExpect(header("realm", "datakow"))
                .andExpect(content().string(records.toJson()))
                .andRespond(withSuccess(getBulkUpdateResponseArrayJson(), MediaType.APPLICATION_JSON_UTF8));
                
        
        JsonInputStreamToIterator<BulkResult> results = client.createBulk(catalogIdentifier, "datakow", Arrays.asList("tag1", "tag2"), records, getMockRecord().getStorage().getObjectIdentities());
        List<BulkResult> expected = getBulkUpdateResponseArray();
        assertEquals(expected.get(0), results.next());
        assertEquals(expected.get(1), results.next());
        assertTrue(!results.hasNext());
    }

    @Test
    public void testJsonPatchById() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records/" + getMockRecord().getStorage().getId());
        
        DotNotationList<JsonPatchOperation> jsonpatch =  new DotNotationList<>();
        jsonpatch.add(JsonPatchOperation.add("/Doc/someProperty", "value"));
        jsonpatch.add(JsonPatchOperation.remove("/Doc/someProperty"));
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.PATCH))
                .andExpect(header("Content-Type", "application/json-patch+json"))
                .andExpect(content().string(jsonpatch.toJson()))
                .andRespond(withSuccess("{}", MediaType.APPLICATION_JSON_UTF8));
                
        
        assertTrue(client.jsonPatchById(catalogIdentifier, getMockRecord().getStorage().getId(), jsonpatch));
    }
    
    @Test
    public void testJsonPatchByQuery() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withSort(sort)
                .withLimit(1)
                .withUpsert(false)
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records");
        
        DotNotationList<JsonPatchOperation> jsonPatch =  new DotNotationList<>();
        jsonPatch.add(JsonPatchOperation.add("/Doc/someProperty", "value"));
        jsonPatch.add(JsonPatchOperation.remove("/Doc/someProperty"));
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.PATCH))
                .andExpect(header("Content-Type", "application/json-patch+json"))
                .andExpect(content().string(jsonPatch.toJson()))
                .andRespond(
                        withSuccess(getBulkUpdateResponseArrayJson(), MediaType.APPLICATION_JSON_UTF8)
                );
                
        
        JsonInputStreamToIterator<BulkResult> results = client.jsonPatchByQuery(catalogIdentifier, fiql, sort, 1, jsonPatch, false);
        List<BulkResult> expected = getBulkUpdateResponseArray();
        assertEquals(expected.get(0), results.next());
        assertEquals(expected.get(1), results.next());
        assertTrue(!results.hasNext());
        
    }
    
    @Test
    public void testMergePatchById() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records/" + getMockRecord().getStorage().getId());
        
        DotNotationMap mergePatch = new DotNotationMap();
        mergePatch.setProperty("Doc.someProp", "someValue");
        mergePatch.setProperty("Doc.anotherProperty", null);
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.PATCH))
                .andExpect(header("Content-Type", "application/merge-patch+json"))
                .andExpect(content().string(mergePatch.toJson()))
                .andRespond(withSuccess("{}", MediaType.APPLICATION_JSON_UTF8));
                
        assertTrue(client.mergePatchById(catalogIdentifier, getMockRecord().getStorage().getId(), mergePatch));
    }
    
    @Test
    public void testMergePatchByQuery() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .withSort(sort)
                .withUpsert(false)
                .withMulti(true)
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records");
        
        DotNotationMap mergePatch = new DotNotationMap();
        mergePatch.setProperty("Doc.someProp", "someValue");
        mergePatch.setProperty("Doc.anotherProperty", null);
        
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.add("Num-Updated", "5");
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.PATCH))
                .andExpect(header("Content-Type", "application/merge-patch+json"))
                .andExpect(content().string(mergePatch.toJson()))
                .andRespond(
                        withStatus(HttpStatus.OK)
                        .body("{\"numUpdated\":5}")
                        .headers(responseHeaders)
                );
                
        
        assertEquals(5, client.mergePatchByQuery(catalogIdentifier, fiql, sort, mergePatch, true, false));
    }
    
    @Test
    public void testDeleteById() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records/" + getMockRecord().getStorage().getId());
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withSuccess("", MediaType.APPLICATION_JSON_UTF8));
                
        assertTrue(client.deleteById(catalogIdentifier, getMockRecord().getStorage().getId()));
    }
    
    @Test
    public void testDeleteByQuery() throws Exception{
        URI uri = MetadataCatalogWebserviceRequest.builder()
                .withQuery(fiql)
                .toUri("http://datakow.com/catalogs/" + catalogIdentifier + "/records");
        
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.add("Num-Deleted", "5");
        
        server = MockRestServiceServer.createServer(template);
        server.expect(requestTo(uri))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withSuccess("", MediaType.APPLICATION_JSON).headers(responseHeaders));
                
        assertEquals(5, client.deleteByQuery(catalogIdentifier, fiql));
    }
    
    private MetadataCatalogRecord getMockRecord(){
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        MetadataCatalogRecordStorage storage = new MetadataCatalogRecordStorage();
        DotNotationMap doc = new DotNotationMap();
        
        storage.setId(recordIdentifier);
        storage.setObjectIdentities(new CatalogIdentityCollection(new CatalogIdentity("DATAKOW_OBJECTS", "1234abc")));
        storage.setPublisher("datakow");
        storage.setRealm("public");
        storage.setTags(Arrays.asList("tag1", "tag2"));
        storage.setPublishDate(new Date(dateEpoch));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2017, 3, 24, 12, 0, 0);
        doc.setProperty("property1", "value1");
        doc.setProperty("property2", 2);
        doc.setProperty("property4.subproperty", "19860602T120000Z");
        doc.setProperty("DateProperty", calendar.getTime());
        record.setStorage(storage);
        record.setDocument(doc);
        return record;
    }
    
    private String getMockRecordJson() throws JsonProcessingException{
        MetadataCatalogRecord record = getMockRecord();
        return record.toJson();
    }
    
    private List<MetadataCatalogRecord> getMockRecordArray() throws JsonProcessingException{
        MetadataCatalogRecord record1 = getMockRecord();
        MetadataCatalogRecord record2 = getMockRecord();
        return Arrays.asList(record1, record2);
    }
    
    private String getMockRecordArrayJson() throws JsonProcessingException{
        return DatakowObjectMapper.getDatakowDateAwareObjectMapper().writeValueAsString(getMockRecordArray());
    }
    
    private DotNotationList<BulkResult> getBulkUpdateResponseArray() throws JsonProcessingException{
        DotNotationList<BulkResult> results =  new DotNotationList<>();
        results.add(new BulkResult(recordIdentifier, 0, "updated"));
        results.add(new BulkResult(recordIdentifier2, 1, "created"));
        return results;
    }
    
    private String getBulkUpdateResponseArrayJson() throws JsonProcessingException{
        return getBulkUpdateResponseArray().toJson();
    }
    
    
    
}
