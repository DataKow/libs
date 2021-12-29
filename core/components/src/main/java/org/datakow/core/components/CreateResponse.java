package org.datakow.core.components;

/**
 * Represents the response from a create operation in the catalogs. 
 * <p>
 * This class contains the identifier of the record created or an error
 * if something went wrong.
 * @author kevin.off
 */
public class CreateResponse {
    
    private boolean isError;
    private Exception innerException;
    private String errorMessage;
    private String recordIdentifier; 

    /**
     * Creates a new successful response
     */
    public CreateResponse(){
        isError = false;
    }
    
    /**
     * Creates a new Successful response for a given record identifier
     * 
     * @param recordIdentifier The record identifier
     */
    public CreateResponse(String recordIdentifier){
        this.recordIdentifier = recordIdentifier;
        this.isError = false;
        this.innerException = null;
        this.errorMessage = null;
    }
    
    /**
     * Creates a new response that may or may not be an error that contains a message.
     * 
     * @param isError True if the response is an error response
     * @param errorMessage The message to include
     */
    public CreateResponse(boolean isError, String errorMessage){
        this.recordIdentifier = null;
        this.isError = isError;
        this.innerException = null;
        this.errorMessage = errorMessage;
    }
    
    /**
     * Creates an error response with an inner exception and a message.
     * 
     * @param innerException the inner exception
     * @param errorMessage the error message
     */
    public CreateResponse(Exception innerException, String errorMessage){
        this.recordIdentifier = null;
        this.isError = true;
        this.innerException = innerException;
        this.errorMessage = errorMessage;
    }
    
    /**
     * Returns true if there was an error
     * @return true if error
     */
    public boolean getIsError() {
        return isError;
    }
    
    /**
     * Sets if the response is an error
     * 
     * @param isError true if error
     */
    public void setIsError(boolean isError) {
        this.isError = isError;
    }

    /**
     * Gets the inner exception
     * 
     * @return the exception or null
     */
    public Exception getInnerException() {
        return innerException;
    }

    /**
     * Sets the inner exception
     * 
     * @param innerException The inner exception
     */
    public void setInnerException(Exception innerException) {
        this.isError = true;
        this.innerException = innerException;
    }

    /**
     * Gets the error message
     * 
     * @return The error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Sets th error message and sets the isError to true.
     * 
     * @param errorMessage The error message
     */
    public void setErrorMessage(String errorMessage) {
        this.isError = true;
        this.errorMessage = errorMessage;
    }

    /**
     * Gets the record identifier that this response represents
     * 
     * @return The Record Identifier
     */
    public String getRecordIdentifier() {
        return recordIdentifier;
    }

    /**
     * Sets the record identifier that this response represents
     * @param recordIdentifier The record identifier
     */
    public void setRecordIdentifier(String recordIdentifier) {
        this.recordIdentifier = recordIdentifier;
    }
    
}
