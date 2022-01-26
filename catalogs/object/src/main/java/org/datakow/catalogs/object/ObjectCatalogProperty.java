package org.datakow.catalogs.object;

/**
 * A class that contains string constants for the property names that exist
 * in all object catalog records.
 * @author kevin.off
 */
public class ObjectCatalogProperty {
    public static final String ID_KEY = "_id";
    public static final String METADATA_KEY = "metadata";
    public static final String CATALOG_IDENTIFIER_KEY = "Catalog-Identifier";
    public static final String RECORD_IDENTIFIER_KEY = "Record-Identifier";
    public static final String IDENTITIES_KEY = "Identities";
    public static final String REALM_KEY = "Realm";
    public static final String PUBLISHER_KEY = "Publisher";
    public static final String PUBLISH_DATE_KEY = "Publish-Date";
    public static final String TAGS_KEY = "Tags";
    public static final String METADATA_IDENTITIES_KEY = "Metadata-Identities";
    public static final String FILENAME_KEY = "filename";
    public static final String LENGTH_KEY = "length";
    public static final String CONTENT_TYPE_KEY = "contentType";
    public static final String MD5_KEY = "md5";
    //public static final String METADATA_CATALOG_IDENTIFIERS_KEY = "Metadata-Catalog-Identifiers";
    
    public static final String IDENTITIES_PATH = METADATA_KEY + "." + IDENTITIES_KEY;
    public static final String METADATA_IDENTITIES_PATH = IDENTITIES_PATH + "." + METADATA_IDENTITIES_KEY;
    public static final String RECORD_IDENTIFIER_PATH = IDENTITIES_PATH + "." + RECORD_IDENTIFIER_KEY;
    public static final String REALM_PATH = IDENTITIES_PATH + "." + REALM_KEY;
    //public static final String METADATA_CATALOG_IDENTIFIERS_PATH = IDENTITIES_PATH + "." + METADATA_CATALOG_IDENTIFIERS_KEY;
    
    
    
}
