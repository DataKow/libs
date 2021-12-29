package org.datakow.catalogs.metadata.database;

/**
 * The enumeration to use when selecting a data coherence to use when getting
 * data from MongoDB.
 * <p>
 * The coherence CONSISTENT is used when you want to read from the primary and
 * AVAILABLE will read form the secondary servers. There is a possibility that 
 * you may encounter stale data due to replication lag if you use SECONDARY
 * @author kevin.off
 */
public enum MetadataDataCoherence {
    AVAILABLE("available"), CONSISTENT("consistent");
    
    private final String coherenceName;

    private MetadataDataCoherence(String name) {
        this.coherenceName = name;
    }

    /**
     * The string representation of the data coherence enumeration
     * 
     * @return The string representation
     */
    public String getCoherenceName(){
        return coherenceName;
    }
    
    /**
     * Gets the DataCoherence by name ignoring case.
     * Default value is AVAILABLE if none can be found
     * @param name The string representation of the enum
     * 
     * @return The data coherence represented by the string or AVAILABLE if not found.
     */
    public static MetadataDataCoherence fromString(String name){
        for(MetadataDataCoherence coherence : MetadataDataCoherence.values()){
            if (coherence.coherenceName.equalsIgnoreCase(name)){
                return coherence;
            }
        }
        return MetadataDataCoherence.AVAILABLE;
    }
    
    @Override
    public String toString(){
        return this.getCoherenceName();
    }
    
}
