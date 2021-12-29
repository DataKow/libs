package org.datakow.exception;

import org.datakow.core.components.DotNotationMap;


/**
 *
 * @author kevin.off
 */
public class DatakowExceptionSource extends DotNotationMap {
    
    public DatakowExceptionSource(String sourceCategory, String sourceDetail){
        this.setProperty(sourceCategory, sourceDetail);
    }
    
}
