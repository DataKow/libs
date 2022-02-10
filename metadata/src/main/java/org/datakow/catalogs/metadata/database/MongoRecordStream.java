package org.datakow.catalogs.metadata.database;

import org.datakow.core.components.DatakowObjectMapper;
import java.io.Closeable;

import com.mongodb.client.MongoCursor;

import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;

/**
 * Acts as a Wrapper for a {@link Cursor} given from MongoDB.
 * <p>
 * This class is used to help with the conversion process from a MongoDB
 * DBObject to the mapped class. If a {@link MongoConverter} is given then it
 * will be used. Otherwise Jackson2Json {@link DatakowObjectMapper} will be used.
 * 
 * @author kevin.off
 * @param <T> The type of object to return on next()
 */
public class MongoRecordStream<T> implements Closeable{
    
    private final MongoConverter converter;
    private final MongoCursor<Document> cursor;
    private final Class<T> clazz;
    
    /**
     * Creates a new MongoRecordStream that will use a Jackson2 ObjectMapper 
     * to convert the DBObject to your class type
     * 
     * @param clazz The class to convert the records to
     * @param cursor The cursor from the MongoDB
     */
    public MongoRecordStream(Class<T> clazz, MongoCursor<Document> cursor){
        this(null, clazz, cursor);
    }
    
    /**
     * Creates a new MongoRecordStream that will use a MongoConverter
     * to convert the DBObject to your class type
     * 
     * @param converter The converter to use for conversion
     * @param clazz The class to convert records to
     * @param cursor The MongoDB cursor
     */
    public MongoRecordStream(MongoConverter converter, Class<T> clazz, MongoCursor<Document> cursor){
        this.converter = converter;
        this.cursor = cursor;
        this.clazz = clazz;
    }
    
    /**
     * Returns true if the iteration has more elements. 
     * (In other words, returns true if Iterator.next would return an element rather than throwing an exception.) 
     * 
     * @return true if the iteration has more elements 
     */
    public boolean hasNext(){
        return cursor.hasNext();
    }
    
    /**
     * Converts the next value in the cursor using the MongoConverter if it was provided, uses ObjectMapper otherwise.
     * 
     * @return The next element converted to the return type
     */
    public T next(){
        if (converter != null){
            return converter.read(clazz, cursor.next());
        }else{
            DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
            return mapper.convertValue(cursor.next(), clazz);
        }
    }
    
    /**
     * Closes the underlying cursor.
     */
    @Override
    public void close(){
        cursor.close();
    }
    
}
