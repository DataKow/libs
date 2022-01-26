package org.datakow.catalogs.metadata.database;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.model.IndexOptions;

import org.datakow.catalogs.metadata.indexes.MongoIndex;
import org.datakow.catalogs.metadata.indexes.MongoIndexField;
import org.datakow.catalogs.metadata.jsonschema.JsonSchema;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.DatakowObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.bson.Document;
import org.datakow.catalogs.metadata.Catalog;
import org.datakow.catalogs.metadata.DataRetentionPolicy;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordDocument;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.TextIndexDefinition;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * The MongoDB DAO used to perform CRUD operations on the catalogs themselves.
 * <p>
 * You can perform operations on catalogs, their indexes, properties, schema, and data retention
 * 
 * @author kevin.off
 */
public class MongoDBMetadataCatalogManagementDao {

    @Autowired
    MongoDBMetadataCatalogDao metaDao;

    @Autowired
    MongoDatabaseFactory factory;
    
    MongoOperations ops;
    
    /**
     * Creates an instance with a configured MongoOperations/MongoTemplate.
     * 
     * @param ops The configured MongoTemplate/MongoOperations object
     */
    public MongoDBMetadataCatalogManagementDao(MongoOperations ops){
        this.ops = ops;
    }
    
    /**
     * Retrieves the catalog's catalog information by its virtual Catalog Identifier.
     * 
     * @param catalogIdentifier The catalog to retrieve
     * @param includeIndexes To include the indexes in the response or not
     * @param includeStats Include size and count or not
     * @return The Catalog object describing the catalog
     * @throws JsonProcessingException If there is an issue parsing any JSON
     */
    public Catalog getCatalogByCatalogIdentifier(String catalogIdentifier, boolean includeIndexes, boolean includeStats) throws JsonProcessingException {
        MetadataCatalogRecord record = metaDao.getById("catalogs", catalogIdentifier, null, MetadataDataCoherence.CONSISTENT);
        return convertToMetadataCatalog(record, includeIndexes, includeStats);
    }
    
    
    /**
     * Retrieves the catalog's catalog information by its MongoDB Collection Name.
     * 
     * @param collectionName The collection name of the catalog
     * @param includeIndexes Whether to include indexes or not
     * @param includeStats Whether to include size and count or not
     * @return The Catalog object describing the catalog
     * @throws IOException on a Mongo error
     */
    public Catalog getCatalogByCollectionName(String collectionName, boolean includeIndexes, boolean includeStats) throws IOException {
        MongoRecordStream<MetadataCatalogRecord> records = metaDao.getByQuery("catalogs", "Doc.Collection-Name==" + collectionName, null, -1, null, MetadataDataCoherence.CONSISTENT);
        if (records.hasNext()){
            MetadataCatalogRecord record = records.next();
            records.close();
            return convertToMetadataCatalog(record, includeIndexes, includeStats);
        }else{
            return null;
        }
    }
    
    /**
     * Retrieves catalog information for all catalogs that are registered.
     * 
     * @param includeIndexes Whether to include the indexes for the catalog in the response or not
     * @param includeStats Whether to include size and count or not
     * @return A list of Catalog objects describing the catalogs
     * @throws IOException On a MongoDB exception
     */
    public List<Catalog> getAllCatalogs(boolean includeIndexes, boolean includeStats) throws IOException {

        MongoRecordStream<MetadataCatalogRecord> records = metaDao.getByQuery("catalogs", "Doc.Catalog-Type==metadata", null, -1, null, MetadataDataCoherence.CONSISTENT);
        List<Catalog> returnList = new ArrayList<>();
        while (records.hasNext()) {
            Catalog catalog = convertToMetadataCatalog(records.next(), includeIndexes, includeStats);
            if (catalog != null){
                returnList.add(catalog);
            }
        }
        records.close();
        return returnList;
    }
    
    /**
     * Creates a new catalog, creates indexes, schema, and a data retention policy.
     * 
     * @param catalogIdentifier The catalog identifier to use
     * @param collectionName The name of the collection to create
     * @param createCollection Whether to create the MongoDB collection or not
     * @param indexStorageObject Whether to create an index on all of the Storage.* properties
     * @param setDefaultRetentionPolicy Whether to add a default retention policy on the catalog
     * @param publisher The username of the creator
     * @return true on success false on exception
     */
    public boolean createCatalog(
            String catalogIdentifier, 
            String collectionName,
            boolean indexStorageObject, 
            String publisher) {
        
        try {
            
            // if (catalogIdentifier.equals("catalogs")  || 
            //     catalogIdentifier.equals("DATAKOW_OBJECTS") || 
            //     catalogIdentifier.equals("subscriptions")) {
            //     initializeDatakow();
            // }
            
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "Creating collection {0}", collectionName);
            if (ops.collectionExists(collectionName)){
                ops.dropCollection(collectionName);
            }
            
            ops.createCollection(collectionName);

            if (catalogRecordExists(catalogIdentifier)){
                metaDao.deleteByQuery("catalogs", "Doc.Catalog-Identifier==" + catalogIdentifier);
            }

            createCatalogRecord(catalogIdentifier, collectionName, publisher);
            
            if (indexStorageObject){
                Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "Indexing storage object for collection {0}", collectionName);
                ensureStorageIndexes(collectionName);
            }
            
            ensureMarkForDeleteIndex(collectionName);

            return true;
            
        } catch (Exception e) {
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.SEVERE, "There was an error creating catalog " + catalogIdentifier, e);
            return false;
        }
    }
    
    private void createCatalogRecord(String catalogIdentifier, String collectionName, String publisher) throws IOException{
        
        //If the catalog information doesn't exist, create it
        if (!catalogRecordExists(catalogIdentifier)){
            Catalog catalog = new Catalog();
            catalog.setCatalogIdentifier(catalogIdentifier);
            catalog.setCollectionName(collectionName);
            if (catalogIdentifier.equalsIgnoreCase("catalogs") || catalogIdentifier.equalsIgnoreCase("subscriptions")){
                catalog.setCatalogType("system");
            }else if(catalogIdentifier.toUpperCase().contains("_OBJECTS")){
                catalog.setCatalogType("object");
            }else{
                catalog.setCatalogType("metadata");
            }

            MetadataCatalogRecord record = new MetadataCatalogRecord();

            MetadataCatalogRecordStorage storage = new MetadataCatalogRecordStorage();
            storage.setId(catalogIdentifier);
            storage.setPublishDate(new Date());
            storage.setPublisher(publisher);
            storage.setRealm("datakow");

            MetadataCatalogRecordDocument doc = MetadataCatalogRecordDocument.fromJson(catalog.toJson());
            //Indexes do not exist in the collection, they are retrieved from mongo
            doc.remove("Indexes");
            doc.remove("Num-Records");
            doc.remove("Size");
            record.setStorage(storage);
            record.setDocument(doc);

            ops.insert(record, "catalogs");
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "Catalog {0} created.", catalogIdentifier);
        }else{
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "Catalog {0} already had a catalog record.", catalogIdentifier);
        }
    }
    
    /**
     * Updates a catalog by replacing it with the new Catalog information given.
     * 
     * @param catalog The catalog information to replace with the existing
     * @param publisher The username of the updater
     * @throws JsonProcessingException If there is invalid JSON in the schema
     * @throws IOException  When reading JSON string fails
     */
    public void updateCatalog(Catalog catalog, String publisher) throws JsonProcessingException, IOException{
        
        MetadataCatalogRecordDocument doc = MetadataCatalogRecordDocument.fromJson(catalog.toJson());
        //Indexes do not exist in the collection, they are retrieved from mongo
        doc.remove("Indexes");
        doc.remove("Num-Records");
        doc.remove("Size");
        JsonSchema schema = catalog.getSchema();
        doc.remove("Schema");
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        record.setDocument(doc);
        metaDao.updateByQuery("catalogs", "Doc.Catalog-Identifier==" + catalog.getCatalogIdentifier(), null, record, publisher, false, false);
        saveSchema(catalog.getCatalogIdentifier(), schema, publisher);
        
    }

    public void initializeDatakow() throws IOException
    {
        if (!ops.collectionExists("catalogs") || !catalogRecordExists("catalogs")){
            createCatalog("catalogs", "catalogs", true, "datakow");
            createIndex("catalogs", new MongoIndex("Doc.Collection-Name", "Doc.Collection-Name", "ASC"));
            createIndex("catalogs", new MongoIndex("Doc.Catalog-Identifier", "Doc.Catalog-Identifier", "ASC"));
        }

        if (!ops.collectionExists("DATAKOW_OBJECTS.files") || !catalogRecordExists("DATAKOW_OBJECTS")){
            createObjectCatalog();
        }

        if (!ops.collectionExists("subscriptions") || !catalogRecordExists("subscriptions")){
            createCatalog("subscriptions", "subscriptions", true, "datakow");
            createIndex("subscriptions", new MongoIndex("Doc.id", "Doc.Id", "DESC"));
            createIndex("subscriptions", new MongoIndex("Doc.catalogIdentifier", "Doc.catalogIdentifier", "DESC"));
            createIndex("subscriptions", new MongoIndex("Doc.endpointIdentifier", "Doc.endpointIdentifier", "DESC"));
        }
    }
    
    /**
     * Creates an index on all of the Storage.* properties.
     * 
     * @param collectionName The name of the collection to add the indexes to
     * @return true if all indexes were created
     */
    private boolean ensureStorageIndexes(String collectionName){
        List<MongoIndex> indexes = new ArrayList<>();
        indexes.add(new MongoIndex("Storage.Realm", "Storage.Realm", "ASC"));
        indexes.add(new MongoIndex("Storage.Publisher", "Storage.Publisher", "ASC"));
        indexes.add(new MongoIndex("Storage.Publish-Date", "Storage.Publish-Date", "DESC"));
        indexes.add(new MongoIndex("Storage.Tags", "Storage.Tags", "ASC"));
        indexes.add(new MongoIndex("Storage.Record-Identifier", "Storage.Record-Identifier", "ASC"));
        indexes.add(new MongoIndex("Storage.Updated-Date", "Storage.Updated-Date", "DESC"));
        indexes.add(new MongoIndex("Storage.Updated-By", "Storage.Updated-By", "ASC"));
        boolean success = true;
        for(MongoIndex index : indexes){
            success = success && createIndex(collectionName, index);
        }
        return success;
    }
    
    private void ensureMarkForDeleteIndex(String collectionName){
        createIndex(collectionName, new MongoIndex("mark_for_delete", "Doc.markForDelete", "ASC"));
    }

    /**
     * Creates the object catalog named DATAKOW_OBJECTS and adds the appropriate indexes
     * @throws IOException
     */
    private void createObjectCatalog() throws IOException{
        
        createCatalogRecord("DATAKOW_OBJECTS", "DATAKOW_OBJECTS", "datakow");

        Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "About to create the Objects files collection");
        if (!ops.collectionExists("DATAKOW_OBJECTS.files")){
            ops.createCollection("DATAKOW_OBJECTS.files");
        }

        Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "About to index Object Files collection");
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("filename", Sort.Direction.ASC).named("filename"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("md5", Sort.Direction.ASC).named("md5"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("contentType", Sort.Direction.ASC).named("contentType"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("length", Sort.Direction.ASC).named("length"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("uploadDate", Sort.Direction.ASC).named("uploadDate"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("metadata.Identities.Record-Identifier", Sort.Direction.ASC).named("metadata.Identities.Record-Identifier"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("metadata.Identities.Realm", Sort.Direction.ASC).named("metadata.Identities.Realm"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("metadata.Identities.Publisher", Sort.Direction.ASC).named("metadata.Identities.Publisher"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("metadata.Identities.Publish-Date", Sort.Direction.ASC).named("metadata.Identities.Publish-Date"));
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(new Index().on("metadata.markForDelete", Sort.Direction.ASC).named("mark_for_delete"));
        
        Index compoundDef = new CompoundIndexDefinition(new Document().append("metadata.Identities.Metadata-Identities.Catalog-Identifier", 1).append("metadata.Identities.Metadata-Identities.Record-Identifier", 1)).named("Catalog-Identifier_Record-Identifier");
        ops.indexOps("DATAKOW_OBJECTS.files").ensureIndex(compoundDef);

        Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "About to create the Objects chunks collection");
        if (!ops.collectionExists("DATAKOW_OBJECTS.chunks")){
            ops.createCollection("DATAKOW_OBJECTS.chunks");
        }
        Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "About to index the Objects chunks collection");

        compoundDef = new CompoundIndexDefinition(new Document().append("files_id", 1).append("n", 1)).named("files_id_1_n_1").unique();
        ops.indexOps("DATAKOW_OBJECTS.chunks").ensureIndex(compoundDef); 
    }

    /**
     * Deletes a catalog by deleting the collection and the record in the catalogs catalog.
     * 
     * @param catalogIdentifier The virtual catalog identifier of the catalog to delete
     * @return true on success
     */
    public boolean deleteCatalog(String catalogIdentifier) {
        try {
            Catalog catalog = getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
            metaDao.deleteById("catalogs", catalogIdentifier);
            ops.dropCollection(catalog.getCollectionName());
            return true;
        } catch (Exception e) {
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.SEVERE, "There was an error deleting a catalog", e);
            return false;
        }
    }
    
    /**
     * DELETES ALL COLLECTIONS IN THE SYSTEM.
     * <p>
     * Uses MongoOperations.dropCollection()
     * 
     * @param exclude The name of any Catalog Identifiers to exclude from the delete.
     * @return true on success false on exception
     */
    public boolean deleteAllCollections(List<String> exclude){
        if (exclude == null){
            exclude = new ArrayList<>();
        }
        try{
            for(String collectionName : ops.getCollectionNames()){
                if (!collectionName.startsWith("system.") && !exclude.contains(collectionName)){
                    ops.dropCollection(collectionName);
                }
            }
            return true;
        }catch(Exception e){
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.SEVERE, "There was an error deleting a collection", e);
            return false;
        }
    }

    /**
     * Converts the retrieved catalog record to a Catalog object.
     * 
     * @param record The metadata catalog record to convert
     * @param includeIndexes Whether to retrieve the indexes
     * @param includeStats Whether to include size and count
     * @return The Catalog object that represents the catalog's information
     * @throws IOException  When reading JSON string fails
     * @throws JsonProcessingException  When reading JSON string fails
     */
    private Catalog convertToMetadataCatalog(MetadataCatalogRecord record, boolean includeIndexes, boolean includeStats) throws JsonProcessingException {
        if (record != null){
            DotNotationMap schema = record.getDocument().getProperty("Schema");
            record.getDocument().remove("Schema");

            Catalog catalog = Catalog.fromJson(record.getDocument().toJson());
            String collectionName = catalog.getCollectionName();
            if (catalog.getCatalogType().equals("object")){
                collectionName = collectionName + ".files";
            }
            if(ops.collectionExists(collectionName)){
                if (schema != null) {
                    schema.setProperty("$schema", schema.getProperty("schema"));
                    schema.remove("schema");
                    catalog.setSchema(JsonSchema.fromJson(schema.toJson()));
                }
                if (includeIndexes){
                    if (record.getStorage() == null){
                        throw new RuntimeException("The Storage property cannot be null");
                    }
                    catalog.setIndexes(this.getIndexes(catalog.getCatalogIdentifier()));
                }
                if (includeStats){
                    Document result = factory.getMongoDatabase().runCommand(Document.parse("{ dbStats: 1, scale: 1}"));
                    if (result.containsKey("count")){
                        catalog.setNumRecords(result.getLong("count"));
                    }
                    if (result.containsKey("size")){
                        catalog.setSize(result.getLong("size"));
                    }
                }

                return catalog;
            }
        }
        return null;
    }

    /**
     * Gets all indexes for a catalog.
     * 
     * @param catalogIdentifier The catalog to get the indexes for
     * @return A list of {@link MongoIndex} objects describing the indexes
     * @throws JsonProcessingException  When reading JSON string fails
     */
    public List<MongoIndex> getIndexes(String catalogIdentifier) throws JsonProcessingException {
        Catalog catalog = getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String collectionName;
        if (catalog.getCatalogType().equals("object")){
            collectionName = catalog.getCollectionName() + ".files";
        }else{
            collectionName = catalog.getCollectionName();
        }
        List<IndexInfo> info = ops.indexOps(collectionName).getIndexInfo();
        List<MongoIndex> indexes = new ArrayList<>();
        for (IndexInfo i : info) {
            if (!i.getName().equals("_id_")) {
                indexes.addAll(MongoIndex.fromJson(mapper.writeValueAsString(i)));
            }
        }

        return indexes;
    }
    
    /**
     * Uses ensure index to create an index in a collection.
     * 
     * @param catalogIdentifier The catalog to create the index in
     * @param index The index to create
     * @return true on success false on exception
     */
    public boolean createIndex(String catalogIdentifier, MongoIndex index) {
        
        try {
            boolean isCompound = index.getIndexFields().size() > 1;
            IndexDefinition idx;
            if (!isCompound && index.getIndexFields().size() == 1) {
                MongoIndexField field = index.getIndexFields().get(0);
                if (field.isGeo()) {
                    idx = new GeospatialIndex(field.getKey()).named(index.getName()).typed(GeoSpatialIndexType.GEO_2DSPHERE);
                } else if (field.isText()) {
                    idx = TextIndexDefinition.builder().onField(field.getKey()).named(index.getName()).build();
                } else {
                    idx = new Index().on(field.getKey(), field.getDirection().equalsIgnoreCase("ASC") ? Sort.Direction.ASC : Sort.Direction.DESC).named(index.getName());
                    if (index.getUnique()){
                        ((Index)idx).unique();
                    }
                }
            } else {
                Document compoundIndexDef = new Document();
                boolean canBeUnique = true;
                for (MongoIndexField field : index.getIndexFields()) {
                    if (field.isGeo()) {
                        compoundIndexDef.append(field.getKey(), "2dsphere");
                        canBeUnique = false;
                    } else if (field.isText()) {
                        compoundIndexDef.append(field.getKey(), "text");
                        canBeUnique = false;
                    } else {
                        compoundIndexDef.append(field.getKey(), field.getDirection().equalsIgnoreCase("ASC") ? 1 : -1);
                    }
                }
                idx = new CompoundIndexDefinition(compoundIndexDef).named(index.getName());
                if (canBeUnique && index.getUnique()){
                    ((CompoundIndexDefinition)idx).unique();
                }
            }
            
            List<MongoIndex> indexes = getIndexes(catalogIdentifier);
            Optional<MongoIndex> existing = indexes.stream().filter(i -> i.getName().equals(index.getName())).findFirst();

            if (existing.isPresent() && !existing.get().equals(index)){
                Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "Index {0} exists but is different. Dropping and recreating.", index.getName());
                deleteIndex(catalogIdentifier, index.getName());
            }

            Catalog catalog = getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
            
            Document keys = idx.getIndexKeys();
            
            ops.getCollection(catalog.getCollectionName()).createIndex(keys, new IndexOptions().background(true));
            
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.FINE, "Index {0} created in the {1} catalog.", new Object[]{index.getName(), catalogIdentifier});
            return true;
        } catch (Exception e) {
            Logger.getLogger(MongoDBMetadataCatalogManagementDao.class.getName()).log(Level.SEVERE, "Error creating index " + index.getName() + " in catalog: " + catalogIdentifier, e);
            return false;
        }
    }

    /**
     * Deletes an index in a catalog by the property name.
     * 
     * @param catalogIdentifier The virtual catalog identifier to delete the index in
     * @param propertyName The name of the property to delete the index for
     * @return returns true or throws an exception
     * @throws IOException on a Mongo error
     */
    public boolean deleteIndex(String catalogIdentifier, String propertyName) throws IOException {
        Catalog catalog = getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
        ops.indexOps(catalog.getCollectionName()).dropIndex(propertyName);
        return true;
    }

    /**
     * Deletes all indexes in a collection.
     * 
     * @param catalogIdentifier The virtual catalog identifier of the catalog
     * @return true or throws an exception
     * @throws IOException on a Mongo error
     */
    public boolean deleteAllIndexes(String catalogIdentifier) throws IOException {
        Catalog catalog = getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
        ops.indexOps(catalog.getCollectionName()).dropAllIndexes();
        return true;
    }

    /**
     * Gets the schema property from a catalog and returns the JsonSchema object that represents it.
     * 
     * @param catalogIdentifier The virtual catalog identifier of the catalog
     * @return The JsonSchema object or null if the catalog or schema does not exist 
     * @throws IOException on a Mongo error
     */
    public JsonSchema getSchema(String catalogIdentifier) throws IOException {

        Catalog catalog = getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
        if (catalog != null) {
            return catalog.getSchema();
        } else {
            return null;
        }
    }
    
    /**
     * Takes the necessary actions required to deal with the $ to save the schema property.
     * 
     * @param catalogIdentifier The catalog to save the schema to
     * @param schema The schema to save
     * @param publisher The username of the updater
     * @return true for success or false if the catalog does not exist or the given schema is null
     * @throws IOException  When reading JSON string fails
     * @throws JsonProcessingException  When reading JSON string fails
     */
    public boolean saveSchema(String catalogIdentifier, JsonSchema schema, String publisher) throws IOException, JsonProcessingException {

        if (schema != null){
            DotNotationMap schemaDoc = DotNotationMap.fromJson(schema.toJson());
            schemaDoc.setProperty("schema", schemaDoc.getProperty("$schema"));
            schemaDoc.remove("$schema");

            Catalog catalog = this.getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
            if (catalog != null) {

                Update update = new Update();
                update.set("Doc.Schema", schemaDoc);
                update.set("Storage.Updated-By", publisher);
                update.set("Storage.Update-Date", new Date());
                ops.updateFirst(new Query(where("Storage.Record-Identifier").is(catalogIdentifier)), update, "catalogs");

                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Deletes the schema property by setting Doc.Schema to null.
     * 
     * @param catalogIdentifier The virtual catalog identifier of the catalog
     * @return true or exception
     */
    public boolean deleteSchema(String catalogIdentifier) {
        MetadataCatalogRecord record = metaDao.getById("catalogs", catalogIdentifier, null, MetadataDataCoherence.CONSISTENT);
        if (record != null) {
            Update update = new Update();
            update.set("Doc.Schema", null);
            ops.updateFirst(new Query(where("Storage.Record-Identifier").is(catalogIdentifier)), update, "catalogs");
        }
        return true;
    }

    /**
     * Gets the data retention policy property from the catalog.
     * 
     * @param catalogIdentifier The virtual catalog identifier of the catalog
     * @return The list of Data Retention policies
     * @throws IOException on a Mongo error
     */
    public List<DataRetentionPolicy> getDataRetentionPolicy(String catalogIdentifier) throws IOException{
        Catalog catalog = getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
        return catalog.getDataRetentionPolicy();
    }
    
    /**
     * Saves the data retention policy to the catalog
     * 
     * @param catalogIdentifier The catalog to save the policy for
     * @param policy The list of policies to save
     * @param publisher The username of the updater
     * @return true or exception
     * @throws IOException When converting the policy to JSON fails
     */
    public boolean saveDataRetentionPolicy(String catalogIdentifier, List<DataRetentionPolicy> policy, String publisher) throws IOException {
        MetadataCatalogRecord record = metaDao.getById("catalogs", catalogIdentifier, null, MetadataDataCoherence.CONSISTENT);
        if (record != null){
            DotNotationList<DotNotationMap> policies = new DotNotationList<>();
            for(DataRetentionPolicy p : policy){
                policies.add(DotNotationMap.fromObject(p));
            }
            Update update = new Update();
            update.set("Doc.Retention-Policy", policies);
            update.set("Storage.Updated-By", publisher);
            update.set("Storage.Update-Date", new Date());
            ops.updateFirst(new Query(where("Storage.Record-Identifier").is(catalogIdentifier)), update, "catalogs");
        }
        return true;
    }
    
    /**
     * Deletes the data retention polity property by setting it to null.
     * 
     * @param catalogIdentifier The virtual catalog identifier of the catalog
     * @param publisher The username of the updater
     * @return true or exception
     */
    public boolean deleteDataRetentionPolicy(String catalogIdentifier, String publisher){
        MetadataCatalogRecord record = metaDao.getById("catalogs", catalogIdentifier, null, MetadataDataCoherence.CONSISTENT);
        if (record != null) {
            Update update = new Update();
            update.set("Doc.Retention-Policy", null);
            update.set("Storage.Updated-By", publisher);
            update.set("Storage.Update-Date", new Date());
            ops.updateFirst(new Query(where("Storage.Record-Identifier").is(catalogIdentifier)), update, "catalogs");
        }
        return true;
    }
    
    /**
     * Gets all metadata catalog identifiers and returns them as a list
     * @return A list of all metadata catalog identifiers
     * @throws IOException On a MongoDB exception 
     */
    public List<String> getMetadataCatalogIdentifiers() throws IOException {
        List<Catalog> names = this.getAllCatalogs(false, false);
        return names.stream()
                .filter(c -> c.getCatalogType().equalsIgnoreCase("metadata"))
                .map(c -> c.getCatalogIdentifier())
                .collect(Collectors.toList());
    }
    
    public boolean catalogRecordExists(String catalogIdentifier){
        return metaDao.count("catalogs", "Doc.Catalog-Identifier == " + catalogIdentifier, 1, MetadataDataCoherence.AVAILABLE) > 0;
    }
    
}
