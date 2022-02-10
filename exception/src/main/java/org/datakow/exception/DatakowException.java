package org.datakow.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DotNotationMap;
import java.io.IOException;


/**
 *
 * @author kevin.off
 */
public class DatakowException extends Exception {
    
    protected String status;
    protected DatakowExceptionSource source;
    protected String title;
    protected String detail;
    
    public DatakowException(String status, DatakowExceptionSource source, String title, String detail, Exception cause){    
        super(cause);
        this.status = status;
        this.source = source;
        this.title = title;
        this.detail = detail;
    }

    public DatakowException(String status, String title, String detail, Exception cause){    
        super(cause);
        this.status = status;
        this.source = null;
        this.title = title;
        this.detail = detail;
    }
    
    public DatakowException(String status, DatakowExceptionSource source, String title, String detail){    
        super();
        this.status = status;
        this.source = source;
        this.title = title;
        this.detail = detail;
    }
    
    public DatakowException(String status, String title, String detail){    
        super();
        this.status = status;
        this.source = null;
        this.title = title;
        this.detail = detail;
    }
    
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public DatakowExceptionSource getSource() {
        return source;
    }

    public void setSource(DatakowExceptionSource source) {
        this.source = source;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }
    
    @Override
    public String getMessage(){
        try {
            return "Status:" + status + ", Source:" + source.toJson() + ", Title: " + title + ", Detail: " + detail;
        } catch (IOException ex) {
            throw new RuntimeException("Error converting source to a JSON string", ex);
        }
    }
    
    public String toJson() throws JsonProcessingException{
        DotNotationMap response = new DotNotationMap();
        response.setProperty("status", this.status);
        response.setProperty("source", this.source);
        response.setProperty("title", this.title);
        response.setProperty("detail", this.detail);
        return response.toJson();
    }
    
}
