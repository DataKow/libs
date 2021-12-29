package org.datakow.messaging.events.events;

/**
 * Enumeration for the Event Type property
 * 
 * @author kevin.off
 */
public class EventType {
    public static final String CATALOG_RECORD = "Catalog-Record";
    public static final String CATALOG_RECORD_ASSOCIATION = "Catalog-Record-Association";
    public static final String CATALOG = "Catalog";
    public static final String SUBSCRIPTION = "Subscription";
    public static final String DATA_FILE = "DataFileEvent";
    public static final String MODEL_INGEST_FILE = "modelingestfile";
    public static final String DATA_CLEANER = "datacleaner";
}
