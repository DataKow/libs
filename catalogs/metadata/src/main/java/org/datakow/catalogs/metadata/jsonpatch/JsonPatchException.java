package org.datakow.catalogs.metadata.jsonpatch;

import org.springframework.core.NestedRuntimeException;

/**
 *
 * @author kevin.off
 */
public class JsonPatchException extends NestedRuntimeException{
    
    String localMessage;
    
    public JsonPatchException(String message){
        super(message);
        localMessage = message;
    }
    public JsonPatchException(String message, Throwable ex){
        super(message, ex);
        localMessage = message;
    }
    
    @Override
    public String getLocalizedMessage(){
        return localMessage;
    }
}
