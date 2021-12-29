package org.datakow.security;

import java.util.Collection;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * Interface that the client should implement to provide user information for
 * the Hard Coded Authentication.
 * 
 * @author kevin.off
 */
public interface HardCodedUserConfiguration {
    
    /**
     * Gets the users information to authenticate against.
     * 
     * @return The list of users
     */
    public Collection<UserDetails> getUsers();
    
}
