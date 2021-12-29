package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.catalogs.metadata.webservice.MetadataCatalogManagementWebserviceClient;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A helper class that stores all of the catalog objects in memory until the specified cache time is reached.
 * <p>
 * This class is available as a bean with the use of the 
 * {@link org.datakow.catalogs.metadata.webservice.configuration.EnableMetadataCatalogWebServiceClient} annotation.
 * 
 * @author kevin.off
 */
public class CatalogRegistry {
    
    Map<String, Catalog> catalogs = new ConcurrentHashMap<>();
    Map<String, Calendar> expireDates = new ConcurrentHashMap<>();
    Calendar allExpireDate = null;
    
    MetadataCatalogManagementWebserviceClient client;
    int cacheTimeInMinutes;
    
    /**
     * Creates an instance of the CatalogRegistry with a given management client to retrieve
     * catalog information and a default cache time in minutes.
     * 
     * @param client The management client to retrieve the catalog information with
     * @param cacheTimeInMinutes The length of time in minutes to cache catalog information
     */
    public CatalogRegistry(MetadataCatalogManagementWebserviceClient client, int cacheTimeInMinutes){
        this.client = client;
        this.cacheTimeInMinutes = cacheTimeInMinutes;
    }
    
    /**
     * Gets catalog information by the virtual catalog identifier.
     * 
     * @param catalogIdentifier The catalog identifier
     * @return The catalog's information or null
     * @throws JsonProcessingException When reading JSON string fails
     */
    public Catalog getByCatalogIdentifier(String catalogIdentifier) throws JsonProcessingException{
        return getFromCache(catalogIdentifier);
    }
    
    public boolean catalogExists(String catalogIdentifier) throws JsonProcessingException{
        return getFromCache(catalogIdentifier) != null;
    }
    
    /**
     * Expires the cache for all catalogs.
     * <p>
     * A new copy will have to be retrieved on the next request.
     */
    public void expireCache(){
        catalogs.clear();
        expireDates.clear();
    }
    
    /**
     * Expires the cache for a catalog.
     * 
     * @param catalogIdentifier The identifier of the catalog to clear the cache for.
     */
    public void expireCache(String catalogIdentifier){
        catalogs.remove(catalogIdentifier);
        expireDates.remove(catalogIdentifier);
    }
    
    /**
     * Gets if the catalog's cache is expired yet
     * 
     * @param catalogIdentifier The identifier of the catalog to check
     * @return True if the catalog's cache is expired
     */
    private boolean isExpired(String catalogIdentifier){
        if(!catalogs.containsKey(catalogIdentifier)){
            return true;
        }
        Calendar expiredDate = expireDates.get(catalogIdentifier);
        if (expiredDate == null){
            return true;
        }
        
        Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        
        return now.after(expiredDate);
    }
    
    public Collection<Catalog> getAllCatalogsFromCache() throws JsonProcessingException{
        Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        if (allExpireDate == null || now.after(allExpireDate)){
            List<Catalog> newCatalogs = client.getAllCatalogs(false, false);
            Calendar expires = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            expires.add(Calendar.MINUTE, cacheTimeInMinutes);
            allExpireDate = expires;
            for(Catalog catalog : newCatalogs){
                catalogs.put(catalog.getCatalogIdentifier(), catalog);
                expireDates.put(catalog.getCatalogIdentifier(), expires);
            }
        }
        return catalogs.values();
    }
    
    /**
     * Internal method to retrieve the catalog from the cache.
     * <p>
     * If the catalog is expired it will retrieve the information from the database
     * and cache the new result.
     * @param catalogIdentifier The identifier of the catalog to retrieve
     * @return The catalog's information
     * @throws IOException When reading the JSON string fails 
     */
    private Catalog getFromCache(String catalogIdentifier) throws JsonProcessingException{
        Catalog catalog;
        if (isExpired(catalogIdentifier)){
            catalog = client.getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
            if (catalog != null){
                Calendar expires = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                expires.add(Calendar.MINUTE, cacheTimeInMinutes);
                catalogs.put(catalogIdentifier, catalog);
                expireDates.put(catalogIdentifier, expires);
            }
        }
        return catalogs.get(catalogIdentifier);
    }
    
}
