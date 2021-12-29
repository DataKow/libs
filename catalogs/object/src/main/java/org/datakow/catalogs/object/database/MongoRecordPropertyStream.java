package org.datakow.catalogs.object.database;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;

import org.datakow.core.components.DotNotationMap;
import org.datakow.fiql.SubscriptionCriteria;
import org.datakow.fiql.SubscriptionFiqlParser;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.datakow.catalogs.object.ObjectCatalogProperty;
import org.springframework.util.StringUtils;

/**
 * This class acts as a wrapper for a MongoDB cursor that allows you to retrieve
 * a specific property value instead of the entire object record when next() is called.
 * 
 * @author kevin.off
 * @param <T> The type of the property
 */
public class MongoRecordPropertyStream<T> implements Closeable, Iterator<T>{
    
    private final MongoCursor<Document> cursor;
    private final String propertyName;
    private final String fiql;
    private final int limit;
    private int numReturned = 0;
    List<String> ids = new ArrayList<>();
    /**
     * Creates a new RecordPropertyStream instance.
     * 
     * @param propertyName The name of the property to use as the value
     * @param fiql The query to use to find the records that match
     * @param limit Maximum number of records to return
     * @param cursor The MongoDB cursor
     */
    public MongoRecordPropertyStream(String propertyName, String fiql, int limit, MongoCursor<Document> cursor){
        this.propertyName = propertyName.replace("metadata.Identities.", "");
        this.fiql = fiql;
        this.cursor = cursor;
        this.limit = limit;
    }
    
    /**
     * Returns true if the cursor has another record.
     * 
     * @return True if there is another record
     */
    @Override
    public boolean hasNext(){
        if (limit < 0 || numReturned < limit){
            if (ids.isEmpty()){
                return cursor.hasNext();
            }else{
                return true;
            }
        }else{
            return false;
        }
    }
    
    /**
     * Advances the cursor to the next item, retrieves the specified property value
     * and returns the value.
     * 
     * @return The value of the specified property in the next record
     */
    @Override
    public T next(){
        
        if (ids.isEmpty()){
            Document obj = cursor.next();
            ids.addAll(findProperty(obj));
        }
        numReturned++;
        if (!ids.isEmpty()){
            return (T)ids.remove(0);
        }else{
            return null;
        }
    }
    
    /**
     * Closes the underlying cursor
     */
    @Override
    public void close(){
        cursor.close();
    }
    
    private List<String> findProperty(Document file){
        
        List<String> properties = new ArrayList<>();
        SubscriptionFiqlParser parser = new SubscriptionFiqlParser();
        SubscriptionCriteria criteria = null;
        if (!StringUtils.hasText(fiql)){
            criteria = null;
        }else{
            criteria = parser.parse(fiql);
        }
        
        DotNotationMap wholeRecord = new DotNotationMap();
        wholeRecord.setProperty("_id", file.get("_id"));
        wholeRecord.setProperty("filename", file.get("filename"));
        wholeRecord.setProperty("aliases", file.get("aliases"));
        wholeRecord.setProperty("chunkSize", file.get("chunkSize"));
        wholeRecord.setProperty("uploadDate", file.get("uploadDate"));
        wholeRecord.setProperty("length", file.get("length"));
        wholeRecord.setProperty("contentType", file.get("contentType"));
        wholeRecord.setProperty("md5", file.get("md5"));
        wholeRecord.setProperty("metadata", file.get("metadata"));
        
        List list = null;
        Object identities = wholeRecord.getProperty(ObjectCatalogProperty.IDENTITIES_PATH);
        if (identities != null){
            if (identities instanceof List){
                list = (List)identities;
            }else{
                list = new ArrayList();
                list.add(identities);
            }
        }
        
        if (list != null){
            if (list.size() == 1){
                if (propertyName.startsWith("/")){
                    properties.add(wholeRecord.getProperty(propertyName.replace("/", "")));
                }else{
                    properties.add(((DotNotationMap)list.get(0)).getProperty(propertyName));
                }
                return properties;
            }
            for(Object identityObj : list){
                DotNotationMap identity = (DotNotationMap)identityObj;
                if(criteria == null || criteria.meetsCriteria(identity)){
                    if (propertyName.startsWith("/")){
                        properties.add(wholeRecord.getProperty(propertyName.replace("/", "")));
                        break;
                    }else{
                        properties.add(identity.getProperty(propertyName));
                    }
                }
            }
        }
        
        if (properties.isEmpty()){
            if (propertyName.startsWith("/")){
                wholeRecord.getProperty(propertyName.replace("/", ""));
            }else{
                if (list != null){
                    for(Object identityObj : list){
                        DotNotationMap identity = (DotNotationMap)identityObj;
                        properties.add(identity.getProperty(propertyName));
                    }
                }
            }
            
        }
        
        return properties;
    }
    
}
