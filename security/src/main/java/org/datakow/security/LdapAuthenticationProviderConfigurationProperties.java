package org.datakow.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration used to setup the LDAP connection and search config
 * @author kevin.off
 */
@ConfigurationProperties(prefix = "datakow.security.ldap")
public class LdapAuthenticationProviderConfigurationProperties {
    
    private String providerUrl;
    private String adminUserDn;
    private String adminUserPassword;
    private String userDnPattern;
    private String groupSearchBase;
    private String groupRoleAttribute;
    private String groupSearchFilter;
    private int userCacheTimeInMinutes;

    
    
    public String getProviderUrl() {
        return providerUrl;
    }

    public void setProviderUrl(String ldapProviderUrl) {
        this.providerUrl = ldapProviderUrl;
    }

    public String getAdminUserDn() {
        return adminUserDn;
    }

    public void setAdminUserDn(String ldapUserDn) {
        this.adminUserDn = ldapUserDn;
    }

    public String getAdminUserPassword() {
        return adminUserPassword;
    }

    public void setAdminUserPassword(String ldapUserPassword) {
        this.adminUserPassword = ldapUserPassword;
    }
    
    public String getUserDnPattern() {
        return userDnPattern;
    }

    public void setUserDnPattern(String usersDn) {
        this.userDnPattern = usersDn;
    }

    public String getGroupSearchBase() {
        return groupSearchBase;
    }

    public void setGroupSearchBase(String groupSearchBase) {
        this.groupSearchBase = groupSearchBase;
    }

    public String getGroupRoleAttribute() {
        return groupRoleAttribute;
    }

    public void setGroupRoleAttribute(String groupRoleAttribute) {
        this.groupRoleAttribute = groupRoleAttribute;
    }

    public String getGroupSearchFilter() {
        return groupSearchFilter;
    }

    public void setGroupSearchFilter(String groupSearchFilter) {
        this.groupSearchFilter = groupSearchFilter;
    }

    public int getUserCacheTimeInMinutes() {
        return userCacheTimeInMinutes;
    }

    public void setUserCacheTimeInMinutes(int userCacheTimeInMinutes) {
        this.userCacheTimeInMinutes = userCacheTimeInMinutes;
    }
    
    
    
}
