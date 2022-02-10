package org.datakow.security;

import org.datakow.security.access.AccessManager;
import org.datakow.security.access.Read;
import org.datakow.security.access.Realm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.datakow.security.access.RealmAccessProvider;
import org.datakow.security.access.Write;

/**
 * Configuration used to setup the hard coded authentication.
 * 
 * @author kevin.off
 */
@Configuration
@EnableConfigurationProperties(HardCodedAuthenticationConfigurationProperties.class)
public class HardCodedAuthenticationConfiguration {
    
    @Autowired
    HardCodedAuthenticationConfigurationProperties props;
    
    @Autowired
    HardCodedUserConfiguration users;
    
    @Autowired 
    RealmAccessProvider accessProvider;
   
  
    /**
     * The authentication provider used to give the implementation its list of users.
     * 
     * @return The auth provider
     */
    @Bean
    public DaoAuthenticationProvider provider(){
        
        DaoAuthenticationProvider provider = new DaoAuthenticationProvider();
        provider.setUserDetailsService(userDetailsService());
        
        return provider;
    }
    
    /**
     * The user details service used to retrieve the list of users.
     * <p>
     * The UserDetailsService ends up calling the {@link HardCodedUserConfiguration} getUsers method
     * 
     * @return The user details service
     */
    @Bean
    public UserDetailsService userDetailsService(){
     
        InMemoryUserDetailsManager manager = new InMemoryUserDetailsManager(users.getUsers());
        return manager;
        
    }
    
    @Bean
    public AccessManager accessManager(){
        AccessManager manager = new AccessManager();
        manager.addAccess(accessProvider.getRealmAccess());
        return manager;
    }
    
}
