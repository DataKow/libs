package org.datakow.catalogs.object.database;

/**
 * Enumeration for the Data Coherence parameter used for searching for objects.
 * <p>
 * The two values are available(use secondary servers) and consistent(use the primary)
 * 
 * @author kevin.off
 */
public enum ObjectDataCoherence {
    AVAILABLE("available"), CONSISTENT("consistent");
    
    private final String coherenceName;

    /**
     * Private constructor to create a new data coherence with a specified name
     * @param name The name of the value
     */
    private ObjectDataCoherence(String name) {
        this.coherenceName = name;
    }
    
    /**
     * Gets the name associated with the enumeration
     * @return The name of the enum
     */
    public String getCoherenceName(){
        return coherenceName;
    }
    
    /**
     * Gets the DataCoherence by name ignoring case.
     * Default value is AVAILABLE if none can be found
     * @param name The name of the enumeration
     * @return The mapped data coherence enum or AVAILABLE if none were found.
     */
    public static ObjectDataCoherence fromString(String name){
        for(ObjectDataCoherence coherence : ObjectDataCoherence.values()){
            if (coherence.coherenceName.equalsIgnoreCase(name)){
                return coherence;
            }
        }
        return ObjectDataCoherence.AVAILABLE;
    }
    
}
