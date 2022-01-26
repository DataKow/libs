package org.datakow.core.components;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kevin.off
 */
public class CatalogIdentityTest {
    
    public CatalogIdentityTest() {
    }

    @Test
    public void testFromUrl() {
        
        String catalog = "http://server.com/catalogs/CATALOG_IDENTIFIER/records/abc-123-a1a2s3d/?3e3ef";
        
        CatalogIdentity identity = CatalogIdentity.fromUrl(catalog);
        
        assertEquals("CATALOG_IDENTIFIER", identity.getCatalogIdentifier());
        assertEquals("abc-123-a1a2s3d", identity.getRecordIdentifier());
        
        catalog = "/catalogs/CATALOG_IDENTIFIER/records/abc-123-a1a2s3d?3e3ef";
        
        identity = CatalogIdentity.fromUrl(catalog);
        
        assertEquals("CATALOG_IDENTIFIER", identity.getCatalogIdentifier());
        assertEquals("abc-123-a1a2s3d", identity.getRecordIdentifier());
        
        catalog = "/catalogs/DATAKOW_OBJECTS/objects/abc-123-a1a2s3d";
        
        identity = CatalogIdentity.fromUrl(catalog);
        
        assertEquals("DATAKOW_OBJECTS", identity.getCatalogIdentifier());
        assertEquals("abc-123-a1a2s3d", identity.getRecordIdentifier());
        
    }
    
}
