package org.datakow.security.access;

import java.util.List;

/**
 *
 * @author kevin.off
 */
public interface RealmAccessProvider {
    
    public List<Realm> getRealmAccess();
    
}
