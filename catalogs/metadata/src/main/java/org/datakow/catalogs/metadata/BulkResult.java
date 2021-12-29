package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.datakow.core.components.DotNotationMap;
import org.springframework.util.StringUtils;

/**
 * Represents a result/response from a bulk operation to MongoDB.
 * 
 * @author kevin.off
 */
@JsonInclude(value = JsonInclude.Include.NON_EMPTY, content = JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BulkResult extends DotNotationMap{
    
    public BulkResult(){
        this.setProperty("Operation.Success", true);
    }
    
    public BulkResult(String recordIdentifier, int sourceIndex, String actionTaken){
        this.setProperty("Storage.Record-Identifier", recordIdentifier);
        this.setProperty("Operation.Source-Index", sourceIndex);
        this.setProperty("Operation.Action-Taken", actionTaken);
        this.setProperty("Operation.Success", true);
    }
    
    public BulkResult(String recordIdentifier, String correlationId, int sourceIndex, String actionTaken){
        this.setProperty("Storage.Record-Identifier", recordIdentifier);
        this.setProperty("Operation.Correlation-Id", correlationId);
        this.setProperty("Operation.Source-Index", sourceIndex);
        this.setProperty("Operation.Action-Taken", actionTaken);
        this.setProperty("Operation.Success", true);
    }
    
    public BulkResult(String recordIdentifier, int sourceIndex, String actionTaken, String errorMessage){
        this.setProperty("Storage.Record-Identifier", recordIdentifier);
        this.setProperty("Operation.Source-Index", sourceIndex);
        this.setProperty("Operation.Action-Taken", actionTaken);
        this.setProperty("Operation.Success", false);
        this.setProperty("Operation.Error-Message", errorMessage);
    }
    
    public BulkResult(String recordIdentifier, String correlationId, int sourceIndex, String actionTaken, String errorMessage){
        this.setProperty("Storage.Record-Identifier", recordIdentifier);
        this.setProperty("Operation.Correlation-Id", correlationId);
        this.setProperty("Operation.Source-Index", sourceIndex);
        this.setProperty("Operation.Action-Taken", actionTaken);
        this.setProperty("Operation.Success", false);
        this.setProperty("Operation.Error-Message", errorMessage);
    }
    
    /**
     * Gets the record identifier of the record that was modified.
     * 
     * @return The record ID
     */
    @JsonIgnore
    public String getRecordIdentifier(){
        return this.getProperty("Storage.Record-Identifier");
    }
    
    /**
     * Sets the record identifier of the record that was modified.
     * 
     * @param recordIdentifier The record ID
     */
    @JsonIgnore
    public void setRecordIdentifier(String recordIdentifier){
        this.setProperty("Storage.Record-Identifier", recordIdentifier);
    }
    
    /**
     * Gets the correlation ID that was set to match up to the record that was modified.
     * This is necessary because for a create operation the record ID is unknown.
     * 
     * @return The correlation ID
     */
    @JsonIgnore
    public String getCorrelationId(){
        return this.getProperty("Operation.Correlation-Id");
    }
    
    /**
     * Sets the correlation ID that was set to match up to the record that was modified.
     * This is necessary because for a create operation the record ID is unknown.
     * 
     * @param correlationId The correlation ID
     */
    @JsonIgnore
    public void setCorrelationId(String correlationId){
        if (StringUtils.hasText(correlationId)){
            this.setProperty("Operation.Correlation-Id", correlationId);
        }else{
            this.remove("Operation.Correlation-Id");
        }
    }
    
    /**
     * Gets the position index of the record that was submitted.
     * 
     * @return The index of the record
     */
    @JsonIgnore
    public int getSourceIndex(){
        return this.getProperty("Operation.Source-Index");
    }
    
    /**
     * Sets the position index of the record that was submitted.
     * 
     * @param sourceIndex The index of the record
     */
    @JsonIgnore
    public void setSourceIndex(int sourceIndex){
        this.setProperty("Operation.Source-Index", sourceIndex);
    }
    
    /**
     * Gets the action that was taken on the record.
     * <p>
     * Could be words such as created, deleted, updated, upserted, etc.
     * 
     * @return The action that was taken
     */
    @JsonIgnore
    public String getActionTaken(){
        return this.getProperty("Operation.Action-Taken");
    }
    
    /**
     * Sets the action that was taken on the record.
     * <p>
     * Could be words such as created, deleted, updated, upserted, etc.
     * 
     * @param actionTaken The action that was taken
     */
    @JsonIgnore
    public void setActionTaken(String actionTaken){
        this.setProperty("Operation.Action-Taken", actionTaken);
    }
    
    /**
     * Gets whether the operation on this record was successful.
     * 
     * @return true on success
     */
    @JsonIgnore
    public boolean getSuccess(){
        return this.getProperty("Operation.Success");
    }
    
    /**
     * Sets whether the operation on this record was successful.
     * 
     * @param success true on success
     */
    @JsonIgnore
    public void setSuccess(boolean success){
        this.setProperty("Operation.Success", success);
    }
    
    /**
     * If the operation was not successful you can get the error message.
     * 
     * @return The error message
     */
    @JsonIgnore
    public String getErrorMessage(){
        return this.getProperty("Operation.Error-Message");
    }
    
    /**
     * If the operation was not successful you can set the error message.
     * 
     * @param success The error message
     */
    @JsonIgnore
    public void setErrorMessage(String success){
        this.setProperty("Operation.Success", false);
        this.setProperty("Operation.Error-Message", success);
    }

}
