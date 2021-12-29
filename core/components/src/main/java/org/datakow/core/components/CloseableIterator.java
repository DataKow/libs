package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author kevin.off
 * @param <T> The type of object that next is supposed to return
 */
public interface CloseableIterator<T> extends Closeable{

    public boolean hasNext();
    public T next() throws IOException, JsonProcessingException;
    public List<T> toList()throws IOException, JsonProcessingException;
    
}
