package org.datakow.catalogs.metadata.jsonpatch;

import org.datakow.catalogs.metadata.database.configuration.MongoMetadataCatalogClientConfiguration;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.DatakowObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Update;

/**
 *
 * @author kevin.off
 */
@RunWith(MockitoJUnitRunner.class)
public class JsonPatchParserTest {
    
    @Mock 
    MongoMappingContext mappingContextMock;
    
    UpdateMapper mapper;
    
    public JsonPatchParserTest() {
    }

    @Before
    public void setUp(){
        MongoDatabaseFactory factory = mock(MongoDatabaseFactory.class);
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
        MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContextMock);
        converter.setCustomConversions(new MongoMetadataCatalogClientConfiguration().customConversions());
        converter.afterPropertiesSet();
        mapper = new UpdateMapper(converter);
    }
    
    
    @Test
    public void testAdd() {
        
        JsonPatchOperation op = JsonPatchOperation.add("/property/anotherProperty", "someValue");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
        assertEquals("someValue", target.getProperty("property.anotherProperty"));
        
        op.setPath("/newProperty");
        op.setValue("newValue");
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
        assertEquals("newValue", target.getProperty("newProperty"));
        
        op.setPath("/newProperty");
        op.setValue(123);
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
        assertEquals(123, (int)target.getProperty("newProperty"));
        
        op.setPath("/array/0");
        op.setValue("stuff");
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
        assertEquals("stuff", target.getProperty("array.0"));
        
        op.setPath("/property/someArray/-");
        op.setValue(789);
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
        assertEquals(789, (int)target.getProperty("property.someArray.3"));
        
        
        op.setPath("/property/someArray/1");
        op.setValue("stuff");
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
        assertEquals("stuff", target.getProperty("property.someArray.1"));
        
        op.setPath("/newProperty");
        op.setValue("{\"subProperty\":123}");
        DotNotationMap map = new DotNotationMap();
        map.setProperty("subProperty", 123);
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
        assertEquals(map, target.getProperty("newProperty"));
    }
    
    @Test(expected=JsonPatchException.class)
    public void testAddIndexOutOfBounds(){
        JsonPatchOperation op = JsonPatchOperation.add("/property/someArray/4", "someValue");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testAddNotAList(){
        JsonPatchOperation op = JsonPatchOperation.add("/property/4", "someValue");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testAddInvalidKey(){
        JsonPatchOperation op = JsonPatchOperation.add("/property/banana/yellow", "someValue");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test
    public void testRemove(){
        JsonPatchOperation operation = JsonPatchOperation.remove("/property/someProperty");
        DotNotationMap target = getTarget();
        assertTrue(target.containsKey("property.someProperty"));
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertFalse(target.containsKey("property.someProperty"));
        
        operation.setPath("/rootProperty");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertFalse(target.containsKey("rootProperty"));
        
        operation.setPath("/array/0");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertTrue(((List)target.getProperty("array")).isEmpty());
        
        operation.setPath("/property/someArray/1");
        assertTrue(((List)target.getProperty("property.someArray")).size() == 3);
        assertEquals(456, (int)target.getProperty("property.someArray.1"));
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertTrue(((List)target.getProperty("property.someArray")).size() == 2);
        assertEquals(789, (int)target.getProperty("property.someArray.1"));
        
    }
    
    @Test(expected=JsonPatchException.class)
    public void testRemoveIndexDoesNotExist(){
        JsonPatchOperation op = JsonPatchOperation.remove("/property/someArray/4");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testRemoveElementNotInArray(){
        JsonPatchOperation op = JsonPatchOperation.remove("/property/someArray/1/property");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testRemoveNotAList(){
        JsonPatchOperation op = JsonPatchOperation.remove("/property/4");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testRemoveInvalidKey(){
        JsonPatchOperation op = JsonPatchOperation.remove("/property/banana/yellow");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testRemoveInvalidKey2(){
        JsonPatchOperation op = JsonPatchOperation.remove("/stuff");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test
    public void testReplace(){
        JsonPatchOperation operation = JsonPatchOperation.replace("/property/someProperty", "newValue");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals("newValue", target.getProperty("property.someProperty"));
        
        operation.setPath("/property/someArray/1");
        operation.setValue(999);
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(999, (int)target.getProperty("property.someArray.1"));
        
        operation.setPath("/rootProperty");
        operation.setValue(999);
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(999, (int)target.getProperty("rootProperty"));
        
        operation.setPath("/array/0");
        operation.setValue(999);
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(999, (int)target.getProperty("property.someArray.1"));
    }
    
    @Test(expected=JsonPatchException.class)
    public void testReplaceElementNotInArray(){
        JsonPatchOperation op = JsonPatchOperation.replace("/property/someArray/1/property", "123");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testReplaceNotAList(){
        JsonPatchOperation op = JsonPatchOperation.replace("/property/4", "123");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testReplaceInvalidKey(){
        JsonPatchOperation op = JsonPatchOperation.replace("/property/banana/yellow", "123");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testReplaceInvalidKey2(){
        JsonPatchOperation op = JsonPatchOperation.replace("/adsf", "123");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test
    public void testCopy(){
        JsonPatchOperation operation = JsonPatchOperation.copy("/property/someArray/1", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(456, (int)target.getProperty("property.someProperty"));
        
        operation.setFrom("/property/someProperty");
        operation.setPath("/property/someArray/1");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(123, (int)target.getProperty("property.someArray.0"));
        assertEquals(456, (int)target.getProperty("property.someArray.1"));
        assertEquals(456, (int)target.getProperty("property.someArray.2"));
        assertEquals(789, (int)target.getProperty("property.someArray.3"));
        
        operation.setFrom("/property/someProperty");
        operation.setPath("/property/anotherProperty");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(456, (int)target.getProperty("property.anotherProperty"));
        
        operation.setFrom("/property/someArray/0");
        operation.setPath("/property/someArray/1");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(123, (int)target.getProperty("property.someArray.0"));
        assertEquals(123, (int)target.getProperty("property.someArray.1"));
        assertEquals(456, (int)target.getProperty("property.someArray.2"));
        assertEquals(456, (int)target.getProperty("property.someArray.3"));
        assertEquals(789, (int)target.getProperty("property.someArray.4"));
        
        operation.setFrom("/rootProperty");
        operation.setPath("/property");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals("rootValue", target.getProperty("property"));
        
        operation.setFrom("/property");
        operation.setPath("/array/-");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals("rootValue", target.getProperty("array.1"));
        
    }
    
    @Test(expected=JsonPatchException.class)
    public void testCopySourceNotInArray(){
        JsonPatchOperation op = JsonPatchOperation.copy("/property/1", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testCopyDestNotAnArray(){
        JsonPatchOperation op = JsonPatchOperation.copy("/property/someArray/1", "/property/anotherProperty/1");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testCopySourceDoesNotExist(){
        JsonPatchOperation op = JsonPatchOperation.copy("/property/banana/yellow", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testCopyDestDoesNotExist(){
        JsonPatchOperation op = JsonPatchOperation.copy("/property/someArray/1", "/property/somePropertyThatIsMissing/anotherProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testCopySourceDoesNotExist2(){
        JsonPatchOperation op = JsonPatchOperation.copy("/bananas", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testCopyDestIndexOutOfBounds(){
        JsonPatchOperation op = JsonPatchOperation.copy("/property/someArray/1", "/property/someArray/10");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test
    public void testMove(){
        JsonPatchOperation operation = JsonPatchOperation.move("/property/someArray/1", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(456, (int)target.getProperty("property.someProperty"));
        assertEquals(123, (int)target.getProperty("property.someArray.0"));
        assertEquals(789, (int)target.getProperty("property.someArray.1"));
        assertFalse(target.containsKey("property.someArray.2"));
        
        operation.setFrom("/property/someProperty");
        operation.setPath("/property/someArray/1");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertFalse(target.containsKey("property.someProperty"));
        assertEquals(123, (int)target.getProperty("property.someArray.0"));
        assertEquals(456, (int)target.getProperty("property.someArray.1"));
        assertEquals(789, (int)target.getProperty("property.someArray.2"));
        assertFalse(target.containsKey("property.someArray.3"));
        
        operation.setFrom("/property/anotherProperty");
        operation.setPath("/property/someProperty");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals("anotherValue", target.getProperty("property.someProperty"));
        assertFalse(target.containsKey("property.anotherProperty"));
        
        operation.setFrom("/property/someArray/0");
        operation.setPath("/property/someArray/1");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals(456, (int)target.getProperty("property.someArray.0"));
        assertEquals(123, (int)target.getProperty("property.someArray.1"));
        assertEquals(789, (int)target.getProperty("property.someArray.2"));
        assertFalse(target.containsKey("property.someArray.3"));
        
        operation.setFrom("/rootProperty");
        operation.setPath("/property");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals("rootValue", target.getProperty("property"));
        assertFalse(target.containsKey("rootProperty"));
        
        operation.setFrom("/property");
        operation.setPath("/asdf");
        JsonPatchParser.applyPatch(Arrays.asList(operation), target);
        assertEquals("rootValue", target.getProperty("asdf"));
        assertFalse(target.containsKey("property"));
        
    }
    
    @Test(expected=JsonPatchException.class)
    public void testMoveSourceNotInArray(){
        JsonPatchOperation op = JsonPatchOperation.move("/property/1", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testMoveDestNotAnArray(){
        JsonPatchOperation op = JsonPatchOperation.move("/property/someArray/1", "/property/anotherProperty/1");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testMoveSourceDoesNotExist(){
        JsonPatchOperation op = JsonPatchOperation.move("/property/banana/yellow", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testMoveSourceDoesNotExist2(){
        JsonPatchOperation op = JsonPatchOperation.move("/adsf", "/property/someProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testMoveDestDoesNotExist(){
        JsonPatchOperation op = JsonPatchOperation.move("/property/someArray/1", "/property/somePropertyThatIsMissing/anotherProperty");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test(expected=JsonPatchException.class)
    public void testMoveDestIndexOutOfBounds(){
        JsonPatchOperation op = JsonPatchOperation.move("/property/someArray/1", "/property/someArray/10");
        DotNotationMap target = getTarget();
        JsonPatchParser.applyPatch(Arrays.asList(op), target);
    }
    
    @Test
    public void testTest(){
        JsonPatchOperation op = JsonPatchOperation.test("/property/someArray/1", "456");
        DotNotationMap target = getTarget();
        assertFalse(JsonPatchParser.applyPatch(Arrays.asList(op), target));
        op.setPath("/rootProperty");
        op.setValue("rootValue");
        assertTrue(JsonPatchParser.applyPatch(Arrays.asList(op), target));
    }
    
    public DotNotationMap getTarget(){
        DotNotationMap target = new DotNotationMap();
        target.setProperty("array", new ArrayList());
        target.setProperty("property.someProperty", "someValue");
        target.setProperty("property.anotherProperty", "anotherValue");
        target.setProperty("property.someArray", new ArrayList());
        target.setProperty("property.someArray.0", 123);
        target.setProperty("property.someArray.1", 456);
        target.setProperty("property.someArray.2", 789);
        target.setProperty("rootProperty", "rootValue");
        target.setProperty("array.0", 0);
        return target;
    }
    
    @Test
    public void testDeserializingJsonPatchOperation() throws IOException{
        String op = "{\"op\":\"add\",\"path\":\"/property/thirdProperty\",\"value\":\"myvalue\"}";
        DatakowObjectMapper.getObjectMapper().readValue(op, JsonPatchOperation.class);
    }
    
}
