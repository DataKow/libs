package org.datakow.security;

import org.datakow.security.access.AccessManager;
import org.datakow.security.access.Read;
import org.datakow.security.access.Realm;
import org.datakow.security.access.RealmAccessProvider;
import org.datakow.security.access.Write;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.security.core.Authentication;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.LdapAuthenticator;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;
import org.springframework.security.ldap.userdetails.LdapAuthoritiesPopulator;

/**
 * Configuration used to setup the LDAP authentication methods.
 * 
 * @author kevin.off
 */
@Configuration
@EnableConfigurationProperties(LdapAuthenticationProviderConfigurationProperties.class)
public class LdapAuthenticationConfiguration {

    @Autowired 
    RealmAccessProvider accessProvider;
    
    @Autowired
    LdapAuthenticationProviderConfigurationProperties props;
    
    /**
     * Creates the Authentication Provider bean that is responsible for connect to
     * and query the LDAP server
     * 
     * @return The authentication provider
     */
    @Bean
    public LdapAuthenticationProvider ldapAuthenticationProvider() {

        DefaultSpringSecurityContextSource s = new DefaultSpringSecurityContextSource(props.getProviderUrl());
        s.setUserDn(props.getAdminUserDn());
        s.setPassword(props.getAdminUserPassword());
        s.afterPropertiesSet();

        BindAuthenticator auth = new BindAuthenticator(s);
        auth.setUserDnPatterns(new String[]{props.getUserDnPattern()});

        DefaultLdapAuthoritiesPopulator authoritiesPopulator = new DefaultLdapAuthoritiesPopulator(s, props.getGroupSearchBase());
        authoritiesPopulator.setGroupRoleAttribute(props.getGroupRoleAttribute());
        authoritiesPopulator.setGroupSearchFilter(props.getGroupSearchFilter());
        
        LdapAuthenticationProvider p = new CachingLdapAuthenticationProvider(auth, authoritiesPopulator);
        
        return p;
    }

    @Bean
    public AccessManager accessManager(){
        AccessManager manager = new AccessManager();
        manager.addAccess(accessProvider.getRealmAccess());
        return manager;
    }
    
    /**
     * An extension of the LdapAuthenticationProvider that provides caching abilities.
     */
    private class CachingLdapAuthenticationProvider extends LdapAuthenticationProvider {

        private final Map<String, Authentication> users = new ConcurrentHashMap<>();
        private final Map<String, Calendar> cacheTime = new ConcurrentHashMap<>();

        /**
         * Creates an instance
         * 
         * @param authenticator Authenticator with search patterns
         * @param populator Populator used to find the user's group and roles
         */
        public CachingLdapAuthenticationProvider(LdapAuthenticator authenticator, LdapAuthoritiesPopulator populator){
            super(authenticator, populator);
        }

        /**
         * Performs an authentication by retrieving the user from cache.
         * 
         * @param authentication The authentication object of the unauthenticated user
         * @return The new authentication object after the auth has been performed
         */
        @Override
        public Authentication authenticate(Authentication authentication){
            String username = (String) authentication.getPrincipal();

            if (!isExpired(username)) {
                Authentication a = getFromCache(username);
                if (a != null) {
                    return a;
                } else {
                    removeFromCache(username);
                }
            } else {
                removeFromCache(username);
            }

            Authentication auth = super.authenticate(authentication);
            addToCache(username, auth);
            return auth;
        }

        /**
         * Determines if the user's information has passed the cache expire time
         * @param userNameThe username to check
         * @return true if the user's information is expired
         */
        private boolean isExpired(String userName) {
            if (cacheTime.containsKey(userName)) {
                Calendar now = Calendar.getInstance();
                Calendar cTime = cacheTime.get(userName);
                now.add(Calendar.MINUTE, props.getUserCacheTimeInMinutes() * -1);
                return now.after(cTime);
            }
            return true;
        }

        /**
         * Gets the user from cache
         * 
         * @param userName The username to get
         * @return The user or null if it does not exist
         */
        private Authentication getFromCache(String userName) {
            return users.get(userName);
        }

        /**
         * Removes a user from cache
         * 
         * @param userName The username ot remove
         */
        private void removeFromCache(String userName) {
            cacheTime.remove(userName);
            users.remove(userName);
        }

        /**
         * Adds a user to the user cache
         * 
         * @param userName The usrename to add
         * @param stuff The authentication stuff to add
         */
        private void addToCache(String userName, Authentication stuff) {
            users.put(userName, stuff);
            cacheTime.put(userName, Calendar.getInstance());
        }

        /**
         * Clears the internal cache
         */
        public void clearCache() {
            cacheTime.clear();
            users.clear();
        }

    }

}
