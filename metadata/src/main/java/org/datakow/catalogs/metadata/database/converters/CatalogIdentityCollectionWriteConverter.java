package org.datakow.catalogs.metadata.database.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.DBObject;
import org.datakow.core.components.CatalogIdentityCollection;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;

/**
 * Mongo converter to convert {@link CatalogIdentityCollection} objects to {@link DBObject}
 * when retrieving records from MongoDB.
 * 
 * @author kevin.off
 */
public class CatalogIdentityCollectionWriteConverter implements Converter<CatalogIdentityCollection, Document>{

    /**
     * Spring MongoDB converter that converts the Identities using the 
     * CatalogIdentityCollection.toJson() function.
     * <p>
     * Uses a combination of toJson and JSON.parse
     * @param identities The identities to convert
     * @return The converted DBObject
     */
    @Override
    public Document convert(CatalogIdentityCollection identities) throws IllegalArgumentException{
        Document identityDbObject = null;
        try {
            identityDbObject = Document.parse(identities.toJson());
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Error converting Mongo Catalog Identity Collection to JSON", ex);
        }
        
        return identityDbObject;
    }
    
}
