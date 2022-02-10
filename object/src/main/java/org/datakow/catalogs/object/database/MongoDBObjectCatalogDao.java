package org.datakow.catalogs.object.database;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.mongodb.BasicDBList;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.UpdateResult;

import org.datakow.configuration.mongo.MongoConfigurationProperties;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.fiql.MongoFiqlParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.bson.Document;
import org.datakow.catalogs.object.ObjectCatalogProperty;
import org.datakow.catalogs.object.ObjectCatalogRecord;
import org.datakow.catalogs.object.ObjectCatalogRecordInput;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.util.StringUtils;


/**
 * The DAO to use when operating on the Object Catalog.
 * 
 * @author kevin.off
 */
public class MongoDBObjectCatalogDao {

    MongoTemplate mongoTemplate;
    GridFSBucket gridFs;
    String bucketName;
    MongoConfigurationProperties props;
    MongoDatabaseFactory factory;
    
    
    /**
     * Creates an instance with all necessary components
     * @param ops The underlying MongoTemplate to use to access collections directly
     * @param gfsOps The underlying GridFsTemplate to use to access the object catalog as a GridFS file store
     * @param props The configuration properties object
     * @param factory The underlying MongoDbFactory used to get a connection
     */
    public MongoDBObjectCatalogDao(
        MongoTemplate ops, 
        GridFsTemplate gfsOps, 
        MongoConfigurationProperties props, 
        MongoDatabaseFactory factory){

        this.mongoTemplate = ops;
        this.bucketName = props.getObjectCatalogCollectionName();
        this.props = props;
        this.factory = factory;
        gridFs = GridFSBuckets.create(mongoTemplate.getDb(), bucketName);
    }
    
    /**
     * Gets a record by its ID
     * 
     * @param recordIdentifier The Id of the record
     * @param coherence The desired data coherence
     * @return The Retrieved record or null
     * @throws JsonProcessingException If there is an issue parsing the object's metadata identity
     */
    public ObjectCatalogRecord getById(String recordIdentifier, ObjectDataCoherence coherence) 
            throws JsonProcessingException, MongoException {
        
        Logger.getLogger(MongoDBObjectCatalogDao.class.getName()).log(Level.INFO, "About to getById {0}. Coherence: {1}", new Object[]{recordIdentifier, coherence.getCoherenceName()});
        
        //Find an object where one of the Identity's Record Identifiers = the record identifier
        ReadPreference preference;
        if (coherence == ObjectDataCoherence.CONSISTENT){
            preference = ReadPreference.primary();
        }else{
            preference = ReadPreference.secondaryPreferred();
        }
        
        GridFSFile file = gridFs
            .withReadPreference(preference)
            .find(new Document(ObjectCatalogProperty.RECORD_IDENTIFIER_PATH, recordIdentifier))
            .first();

        if (file != null){
            //Build the product object out of the MongoDBFile
            ObjectCatalogRecord product = toObjectCatalogRecord(file, recordIdentifier);
            return product;
        }else{
            return null;
        }
    }

    /**
     * Retrieves an object by a query
     * 
     * @param fiql The FIQL query to use
     * @param coherence The desired data coherence
     * @return A stream of records found
     */
    public MongoRecordPropertyStream<String> getByQuery(String fiql, ObjectDataCoherence coherence){
        return getByQuery(fiql, null, -1, coherence);
    }
    
    /**
     * Retrieves an object by a query
     * 
     * @param fiql The FIQL query to use
     * @param sortString The sort string: property ASC|DESC, ...
     * @param coherence The desired data coherence
     * @return A stream of records found
     */
    public MongoRecordPropertyStream<String> getByQuery(String fiql, String sortString, ObjectDataCoherence coherence){
        return getByQuery(fiql, sortString, -1, coherence);
    }
    
    /**
     * Retrieves an object by a query
     * 
     * @param fiql The FIQL query to use
     * @param limit The maximum number of records to return
     * @param coherence The desired data coherence
     * @return A stream of records found
     */
    public MongoRecordPropertyStream<String> getByQuery(String fiql, int limit, ObjectDataCoherence coherence){
        return getByQuery(fiql, null, limit, coherence);
    }
    
    /**
     * Retrieves an object by a query
     * 
     * @param fiql The FIQL query to use
     * @param sortString The sort string: property ASC|DESC, ...
     * @param limit The maximum number of records to return
     * @param coherence The desired data coherence
     * @return A stream of records found
     */
    public MongoRecordPropertyStream<String> getByQuery(String fiql, String sortString, int limit, ObjectDataCoherence coherence){
        
        ReadPreference readPreference;
        if (coherence == ObjectDataCoherence.CONSISTENT){
            readPreference = ReadPreference.primary();
        }else{
            readPreference = ReadPreference.secondaryPreferred();
        }
        
        Document criteriaDBObject;
        if (StringUtils.hasText(fiql)){
            MongoFiqlParser parser = new MongoFiqlParser();
            Criteria criteria = parser.parse(fiql, ObjectCatalogProperty.IDENTITIES_PATH);
            criteriaDBObject = criteria.getCriteriaObject();
        }else{
            criteriaDBObject = new Document();
        }
        
        FindIterable<Document> iterable = getFilesCollection().withReadPreference(readPreference).find(criteriaDBObject).limit(limit);

        
        if (limit > 0){
            iterable.limit(limit);
        }
        if (StringUtils.hasText(sortString)){
            iterable.sort(getSortObject(sortString));
        }


        return new MongoRecordPropertyStream<String>(ObjectCatalogProperty.RECORD_IDENTIFIER_KEY, fiql, limit, iterable.cursor());
        
    }
    
    /**
     * Converts the sort string into a MongoDB Document to use in the query
     * @param sortString The sort string to convert
     * @return The converted sort Document
     */
    private Document getSortObject(String sortString){
        
        Document sort = new Document();
        
        List<String> sortList = Arrays.asList(sortString.split(","));

        sortList.stream().forEach((String s)->{
            String[] sortListParts = s.split(" ", 2);
            if (sortListParts.length == 1){
                sort.append(sortListParts[0], 1);
            }else if (sortListParts.length == 2){
                sort.append(sortListParts[0], sortListParts[1].equalsIgnoreCase("DESC") ? -1 : 1);
            }
        });
        
        return sort;
    }
    
    /**
     * Deletes an object by ID
     * @param recordIdentifier The ID of the object to delete
     */
    public void deleteById(String recordIdentifier) {
        Logger.getLogger(MongoDBObjectCatalogDao.class.getName()).log(Level.INFO, 
                "About to delete record {0}", 
                new Object[]{recordIdentifier});
        
        FindIterable<Document> records = getFilesCollection()
            .withReadPreference(ReadPreference.primary())
            .find(new Document(ObjectCatalogProperty.RECORD_IDENTIFIER_PATH, recordIdentifier))
            .limit(1);

        while(records.cursor().hasNext()){
            Object id = records.cursor().next().get("_id");
            getFilesCollection().deleteOne(new Document(new Document("_id", id)));
            getChunksCollection().deleteMany(new Document(new Document("files_id", id)));
        }
        
    }
    
    public int deleteByQuery(String fiql, String sortString, int limit){
        Document mappedQuery = getMappedQuery(fiql);
        boolean more = true;
        int total = 0;
        FindIterable<Document> records = null;
        try{
            while(more){
                
                records = getFilesCollection()
                    .withReadPreference(ReadPreference.primary())
                    .find(mappedQuery)
                    .projection(new Document("_id", 1))
                    .limit(200);

                if (StringUtils.hasText(sortString)){
                    records.sort(getSortObject(sortString));
                }
                int count = 0;
                BulkOperations filesBulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, bucketName + ".files");
                BulkOperations chunksBulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, bucketName + ".chunks");
                while(records.cursor().hasNext()){
                    Object id = records.cursor().next().get("_id");
                    chunksBulkOps.remove(new BasicQuery(new Document("files_id", id)));
                    filesBulkOps.remove(new BasicQuery(new Document("_id", id)));
                    count++;
                    total++;
                    if (total == limit){
                        break;
                    }
                }
                if (count > 0){
                    filesBulkOps.execute();
                    chunksBulkOps.execute();
                }
                if (limit > 0 && total == limit){
                    more = false;
                }else if (count < 200){
                    more = false;
                }
            }
        }finally{
            if (records != null){
                records.cursor().close();
            }
        }
        return total;
    }
    
    /**
     * Creates a new record in the object catalog
     * 
     * @param object The object to create
     * @return An object describing the result of the operation
     * @throws JsonProcessingException If there is a problem parsing the JSON in the objects identity object
     */
    public CatalogIdentity create(ObjectCatalogRecordInput object) throws JsonProcessingException {
        
        String recordIdentifier = UUID.randomUUID().toString();
        Logger.getLogger(MongoDBObjectCatalogDao.class.getName()).log(Level.INFO, 
                "About to create record {0}",
                new Object[]{recordIdentifier});

        Document identityDocument = toObjectIdentityDocument(object, recordIdentifier);;
        
        GridFSUploadOptions options = new GridFSUploadOptions()
                       .metadata(
                            new Document("contentType", object.getContentType())
                            .append(ObjectCatalogProperty.IDENTITIES_KEY, identityDocument));

        gridFs.uploadFromStream(UUID.randomUUID().toString(), object.getData(), options);
        
        return new CatalogIdentity(bucketName, recordIdentifier);
        
    }
    
    public UpdateResult mergePatchByQuery(
        String catalogIdentifier, 
        String fiql, 
        DotNotationMap mergePatch, 
        boolean updateMulti, 
        String publisher){
        
        Map<String, Object> flattened = mergePatch.flatten();
        Update update = new Update();
        for(String key : flattened.keySet()){
            Object value = flattened.get(key);
            if (!key.startsWith(ObjectCatalogProperty.METADATA_KEY)){
                key = ObjectCatalogProperty.METADATA_KEY + "." + key;
            }
            if (value == null){
                update.unset(key);
            }else{
                update.set(key, value);
            }
        }

        if (updateMulti){
            return getFilesCollection().updateMany(getMappedQuery(fiql), update.getUpdateObject());
        } else {
            return getFilesCollection().updateOne(getMappedQuery(fiql), update.getUpdateObject());
        }
        
    }
    
    protected Document getMappedQuery(String fiql){
        Query query = makeQuery(fiql);
        QueryMapper mapper = new QueryMapper(mongoTemplate.getConverter());
        Document mappedQuery = mapper.getMappedObject(query.getQueryObject(), Optional.empty());
        return mappedQuery;
    }
    
    protected Query makeQuery(String fiql){
        Query query;
        if (StringUtils.hasText(fiql)){
            MongoFiqlParser parser = new MongoFiqlParser();
            Criteria criteria = parser.parse(fiql, ObjectCatalogProperty.IDENTITIES_PATH);
            query = new Query(criteria);
        }else{
            query = new Query();
        }
        return query;
    }
    
    /**
     * Converts an Object Catalog Record object to a Document so it can be inserted
     * @param input The source object catalog record to convert
     * @param recordIdentifier The RecordID to use in the record
     * @return The converted Document
     * @throws JsonProcessingException If reading JSON fails
     */
    private Document toObjectIdentityDocument(ObjectCatalogRecordInput input, String recordIdentifier) throws JsonProcessingException{
        Document doc = new Document();
 
        if (input.getRealm() != null && !input.getRealm().isEmpty()){
            doc.append(ObjectCatalogProperty.REALM_KEY, input.getRealm());
        }
        if (input.getPublisher() != null && !input.getPublisher().isEmpty()){
            doc.append(ObjectCatalogProperty.PUBLISHER_KEY, input.getPublisher());
        }
        if (recordIdentifier != null && !recordIdentifier.isEmpty()){
            doc.append(ObjectCatalogProperty.RECORD_IDENTIFIER_KEY, recordIdentifier);
        }
        doc.append(ObjectCatalogProperty.PUBLISH_DATE_KEY, new Date());
        if (input.getTags() != null && !input.getTags().isEmpty()){
            doc.append(ObjectCatalogProperty.TAGS_KEY, input.getTags());
        }
        if (input.getObjectMetadataIdentities() != null && !input.getObjectMetadataIdentities().isEmpty()){
            doc.append(ObjectCatalogProperty.METADATA_IDENTITIES_KEY, DotNotationList.fromJson(input.getObjectMetadataIdentities().toJson()));
        }
        // if (input.getMetadataCatalogIdentifiers()!= null && !input.getMetadataCatalogIdentifiers().isEmpty()){
        //     doc.append(ObjectCatalogProperty.METADATA_CATALOG_IDENTIFIERS_KEY, input.getMetadataCatalogIdentifiers());
        // }
        return doc;
    }
    
    
    
    /**
     * Converts a GridFSDBFile to an Object Catalog Record after it is read from the DB.
     * 
     * @param file The source GridFSDBFile
     * @param recordIdentifier the ID to use on the new record
     * @return The converted record
     * @throws JsonProcessingException If reading JSON fails
     */
    private ObjectCatalogRecord toObjectCatalogRecord(
        GridFSFile file, 
        String recordIdentifier) throws JsonProcessingException {

        ObjectCatalogRecord catalogObj = new ObjectCatalogRecord();
 
        if (file.getMetadata() != null){
            //If there are identities (there better be or you have other problems)
            Document metadata = (Document)file.getMetadata();
            if (metadata.containsKey(ObjectCatalogProperty.IDENTITIES_KEY)){
                //Get the identities from the identities array
                BasicDBList identities;
                if (metadata.get(ObjectCatalogProperty.IDENTITIES_KEY) instanceof BasicDBList){
                    identities = (BasicDBList)metadata.get(ObjectCatalogProperty.IDENTITIES_KEY);
                }else{
                    identities = new BasicDBList();
                    identities.add(metadata.get(ObjectCatalogProperty.IDENTITIES_KEY));
                }
                //Loop through the identities and find the one to use
                boolean foundIdentity = false;
                for(Object ident : identities){
                    Document identity = (Document)ident;
                    if (identity.containsKey(ObjectCatalogProperty.RECORD_IDENTIFIER_KEY)){
                        if (identity.getString(ObjectCatalogProperty.RECORD_IDENTIFIER_KEY).equalsIgnoreCase(recordIdentifier)){
                            foundIdentity = true;
                            //Build the ObjectCatalogRecord based on the desired identity
                            if (identity.containsKey(ObjectCatalogProperty.RECORD_IDENTIFIER_KEY)){
                                catalogObj.setId(identity.getString(ObjectCatalogProperty.RECORD_IDENTIFIER_KEY));
                            }
                            if (identity.containsKey(ObjectCatalogProperty.REALM_KEY)){
                                catalogObj.setRealm(identity.getString(ObjectCatalogProperty.REALM_KEY));
                            }
                            if (identity.containsKey(ObjectCatalogProperty.PUBLISHER_KEY)){
                                catalogObj.setPublisher(identity.getString(ObjectCatalogProperty.PUBLISHER_KEY));
                            }
                            if (identity.containsKey(ObjectCatalogProperty.PUBLISH_DATE_KEY)){
                                catalogObj.setPublishDate(identity.getDate(ObjectCatalogProperty.PUBLISH_DATE_KEY));
                            }
                            if (identity.containsKey(ObjectCatalogProperty.TAGS_KEY)){
                                BasicDBList tagList = (BasicDBList)identity.get(ObjectCatalogProperty.TAGS_KEY);
                                if(tagList != null){
                                    for(Object tagObj : tagList){
                                        catalogObj.getTags().add((String)tagObj);
                                    }
                                }
                            }
                            // if (identity.containsKey(ObjectCatalogProperty.METADATA_CATALOG_IDENTIFIERS_KEY)){
                            //     BasicDBList catalogIdentifierList = (BasicDBList)identity.get(ObjectCatalogProperty.METADATA_CATALOG_IDENTIFIERS_KEY);
                            //     if(catalogIdentifierList != null && !catalogIdentifierList.isEmpty()){
                            //         List<String> identifiers = new ArrayList<>();
                            //         for(Object catalogIdentifier : catalogIdentifierList){
                            //             identifiers.add((String)catalogIdentifier);
                            //         }
                            //         catalogObj.setMetadataCatalogIdentifiers(identifiers);
                            //     }
                            // }
                            if (identity.containsKey(ObjectCatalogProperty.METADATA_IDENTITIES_KEY)){
                                BasicDBList metadataIdentities = new BasicDBList();
                                metadataIdentities.addAll((ArrayList<CatalogIdentity>)identity.get(ObjectCatalogProperty.METADATA_IDENTITIES_KEY));
                                for(Object i : metadataIdentities){
                                    catalogObj.getObjectMetadataIdentities().add(CatalogIdentity.fromJson(((Document)i).toJson()));
                                }
                            }
                            break;
                        }
                    }else{
                        Logger.getLogger(MongoDBObjectCatalogDao.class.getName()).log(Level.SEVERE, 
                                "During the construction of Object Catalog Record {0} it was discovered "
                                        + "that an Identity in object record file {1} is improperly "
                                        + "formatted and does not have a Record-Identifier", 
                                new Object[]{recordIdentifier, file.getFilename()});
                        return null;
                    }
                }
                if (foundIdentity){
                    catalogObj.setContentLength(file.getLength());
        
                    // String md5Hex = file.getMD5();
                    // byte[] md5Binary = DatatypeConverter.parseHexBinary(md5Hex);
                    // String md5Base64String = DatatypeConverter.printBase64Binary(md5Binary);
                    // catalogObj.setContentMD5(md5Base64String);
                    
                    catalogObj.setContentType(file.getMetadata().getString("contentType"));
                    catalogObj.setData(gridFs.openDownloadStream(file.getFilename()));

                    return catalogObj;
                }else{
                    Logger.getLogger(MongoDBObjectCatalogDao.class.getName()).log(Level.SEVERE, 
                            "During the construction of Object Catalog Record {0} it was "
                                    + "discovered that The Object Catalog Record file {1} does not have an Identity "
                                    + "for Record-Identifier: {2}", 
                            new Object[]{recordIdentifier, file.getFilename(), recordIdentifier});
                    return null;
                }
            }else{
                Logger.getLogger(MongoDBObjectCatalogDao.class.getName()).log(Level.SEVERE, 
                        "During the construction of Object Catalog Record {0} it was discovered that The Object Catalog Record file {1} "
                                + "is improperly formatted and does not have an Identities array.",
                        new Object[]{recordIdentifier, file.getFilename()});
                return null;
            }
        }else{
            Logger.getLogger(MongoDBObjectCatalogDao.class.getName()).log(Level.SEVERE, 
                    "During the construction of Object Catalog Record {0} "
                            + "it was discovered that The Object Catalog Record file {1} "
                            + "is improperly formatted and does not have a metadata object in its files record.", 
                    new Object[]{recordIdentifier, file.getFilename()});
            return null;
        }
          
    }
    
    private MongoCollection<Document> getFilesCollection(){
        return mongoTemplate.getCollection(bucketName + ".files");
    }
    
    private MongoCollection<Document> getChunksCollection(){
        return mongoTemplate.getCollection(bucketName + ".chunks");
    }
    
    /**
     * Gets the name of all object catalogs
     * 
     * @return the list of catalog names
     */
    public List<String> getObjectCatalogNames(){
        Set<String> names = mongoTemplate.getCollectionNames();
        return names.stream()
                .filter((n)-> n.contains(".files"))
                .map((n)->n.replace(".files", ""))
                .collect(Collectors.toList());
    }
    
}
