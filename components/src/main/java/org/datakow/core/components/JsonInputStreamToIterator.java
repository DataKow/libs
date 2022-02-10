package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author kevin.off
 */
public class JsonInputStreamToIterator<T> implements CloseableIterator<T>{
    
    private final InputStream inputStream;
    private final Class<T> clazz;
    
    DatakowObjectMapper mapper;
    JsonParser parser;
    
    JsonToken currentToken;
    
    public static <T> JsonInputStreamToIterator<T> makeIterator(InputStream is, Class<T> clazz) throws IOException, JsonParseException{
        return new JsonInputStreamToIterator<>(is, clazz).initializeJsonStream();
    }
    
    private JsonInputStreamToIterator(InputStream is, Class<T> clazz) {
        this.inputStream = is;
        this.clazz = clazz;
    }

    @Override
    public boolean hasNext() {
        if (this.inputStream != null && currentToken != JsonToken.END_ARRAY && parser.hasCurrentToken()){
            return true;
        }else{
            IOUtils.closeQuietly(this.inputStream);
            return false;
        }
    }

    @Override
    public T next() throws IOException, JsonProcessingException {
        
        T record;
        
        currentToken = parser.getCurrentToken();
      
        record = (T)mapper.readValue(parser, clazz);

        currentToken = parser.nextToken();
        
        //Close input stream if we are at the end
        hasNext();
        
        return record;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.inputStream);
    }
    
    private JsonInputStreamToIterator<T> initializeJsonStream() throws IOException, JsonParseException{
        
        if (this.inputStream != null){
            mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
            parser = mapper.getFactory().createParser(this.inputStream);
            currentToken = parser.nextToken();
            //Increment the parser to the first open object character {
            while(parser.hasCurrentToken() && (currentToken != JsonToken.START_OBJECT && currentToken != JsonToken.VALUE_STRING && currentToken != JsonToken.VALUE_NUMBER_INT &&
                    currentToken != JsonToken.VALUE_NUMBER_FLOAT && currentToken != JsonToken.VALUE_TRUE && currentToken != JsonToken.VALUE_FALSE && 
                    currentToken != JsonToken.VALUE_NULL)){
                currentToken = parser.nextToken();
            }
        }
        return this;
    }

    @Override
    public List<T> toList() throws JsonProcessingException, IOException {
        List<T> list = new ArrayList<>();
        while(hasNext()){
            list.add(next());
        }
        return list;
    }
    
}
