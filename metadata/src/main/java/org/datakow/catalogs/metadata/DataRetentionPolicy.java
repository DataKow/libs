package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DatakowObjectMapper;

/**
 * Class that represents the data retention policy of a catalog.
 * 
 * @author kevin.off
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataRetentionPolicy {
    
    private int retentionPeriodInDays;
    private String retentionDateKey;
    private String retentionFilter;

    /**
     * Gets the number of days to retain the data for the catalog.
     * 
     * @return The number of days to retain the data
     */
    public int getRetentionPeriodInDays() {
        return retentionPeriodInDays;
    }

    /**
     * Sets the number of days to retain the data for the catalog.
     * 
     * @param retentionPeriodInDays The number of days to retain the data
     */
    public void setRetentionPeriodInDays(int retentionPeriodInDays) {
        this.retentionPeriodInDays = retentionPeriodInDays;
    }

    /**
     * Gets the property name of the date to use to compare the current time against
     * to determine if the data should be deleted.
     * 
     * @return The property name of the date
     */
    public String getRetentionDateKey() {
        return retentionDateKey;
    }

    /**
     * Sets the property name of the date to use to compare the current time against
     * to determine if the data should be deleted.
     * 
     * @param retentionDateKey The property name of the date
     */
    public void setRetentionDateKey(String retentionDateKey) {
        this.retentionDateKey = retentionDateKey;
    }

    /**
     * Gets the additional FIQL query to use to narrow down the results to delete.
     * 
     * @return The additional FIQL query
     */
    public String getRetentionFilter() {
        return retentionFilter;
    }

    /**
     * Sets the additional FIQL query to use to narrow down the results to delete.
     * 
     * @param retentionFilter The additional FIQL query
     */
    public void setRetentionFilter(String retentionFilter) {
        this.retentionFilter = retentionFilter;
    }
    
    public static DataRetentionPolicy fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        return mapper.readValue(json, DataRetentionPolicy.class);
    }
    
    public String toJson() throws JsonProcessingException {
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        return mapper.writeValueAsString(this);
    }
    
}
