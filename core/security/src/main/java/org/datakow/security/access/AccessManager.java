package org.datakow.security.access;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.util.StringUtils;

/**
 *
 * @author kevin.off
 */
public class AccessManager {
    
    private final Map<String, Realm> realms = new HashMap<>();
    
    public AccessManager(){
       
    }
    
    public Collection<String> getReadingRealms(Authentication auth){
        HashSet<String> readingRealms = new HashSet<>();
        for(GrantedAuthority s : auth.getAuthorities()){
            readingRealms.addAll(getReadingRealms(s.getAuthority()));
        }
        if (readingRealms.contains("*")){
            readingRealms.clear();
            readingRealms.add("*");
        }
        return readingRealms;
    }
    
    public Collection<String> getReadingRealms(String role){
        HashSet<String> readingRealms = new HashSet<>();
        for(Realm realm : realms.values()){
            for(String roleName : realm.getReadingRoles()){
                if (role.equals(roleName) || roleName.equals("*")){
                    readingRealms.add(realm.getName());
                    break;
                }
            }
        }
        if (readingRealms.contains("*")){
            readingRealms.clear();
            readingRealms.add("*");
        }
        return readingRealms;
    }
    
    public Collection<String> getWritingRealms(Authentication auth){
        HashSet<String> writingRealms = new HashSet<>();
        for(GrantedAuthority s : auth.getAuthorities()){
            writingRealms.addAll(getWritingRealms(s.getAuthority()));
        }
        if (writingRealms.contains("*")){
            writingRealms.clear();
            writingRealms.add("*");
        }
        return writingRealms;
    }
    
    public Collection<String> getWritingRealms(String role){
        HashSet<String> writingRealms = new HashSet<>();
        for(Realm realm : realms.values()){
            for(String roleName : realm.getWritingRoles()){
                if (role.equals(roleName) || roleName.equals("*")){
                    writingRealms.add(realm.getName());
                    break;
                }
            }
        }
        if (writingRealms.contains("*")){
            writingRealms.clear();
            writingRealms.add("*");
        }
        return writingRealms;
    }
    
    protected Collection<String> getReadingRoles(String realm){
        HashSet<String> readingRoles = new HashSet<>();
        if (realms.containsKey(realm)){
            readingRoles.addAll(realms.get(realm).getReadingRoles());
        }
        if (realms.containsKey("*")){
            readingRoles.addAll(realms.get("*").getReadingRoles());
        }
        if (readingRoles.contains("*")){
            readingRoles.clear();
            readingRoles.add("*");
        }
        return readingRoles;
    }
    
    protected Collection<String> getWritingRoles(String realm){
        HashSet<String> writingRoles = new HashSet<>();
        if (realms.containsKey(realm)){
            writingRoles.addAll(realms.get(realm).getWritingRoles());
        }
        if (realms.containsKey("*")){
            writingRoles.addAll(realms.get("*").getWritingRoles());
        }
        if (writingRoles.contains("*")){
            writingRoles.clear();
            writingRoles.add("*");
        }
        return writingRoles;
    }
    
    public boolean canRead(Authentication auth, String recordRealm, String recordOwner){
        Collection<String> userRoles = new HashSet<>();
        for(GrantedAuthority ga : auth.getAuthorities()){
            userRoles.add(ga.getAuthority());
        }
        return canRead(
                auth.getName(), 
                userRoles, 
                recordRealm,
                recordOwner);
    }
    
    public boolean canRead(String userName, Collection<String> userRoles, String recordRealm, String recordOwner){
        
        Realm emptyRealm = new Realm("empty");
        Realm theRealm = realms.getOrDefault(recordRealm, emptyRealm);
        Realm magicRealm = realms.getOrDefault("*", emptyRealm);
        return hasAccess(userName, userRoles, theRealm.getReadingRoles(), magicRealm.getReadingRoles(), recordOwner);
        
    }
    
    public boolean canReadFromAnyRealm(Authentication auth){
        Collection<String> userRoles = new HashSet<>();
        for(GrantedAuthority ga : auth.getAuthorities()){
            userRoles.add(ga.getAuthority());
        }
        return canReadFromAnyRealm(userRoles);
    }
    
    public boolean canReadFromAnyRealm(Collection<String> userRoles){
        Realm emptyRealm = new Realm("empty");
        Realm magicRealm = realms.getOrDefault("*", emptyRealm);
        return hasAccessToAllRealms(userRoles, magicRealm.getReadingRoles());
    }
    
    public boolean canWrite(Authentication auth, String realm, String recordOwner){
        Collection<String> userRoles = new HashSet<>();
        for(GrantedAuthority ga : auth.getAuthorities()){
            userRoles.add(ga.getAuthority());
        }
        return canWrite(auth.getName(),
                userRoles, 
                realm,
                recordOwner);
    } 
    
    public boolean canWrite(String userName, Collection<String> userRoles, String recordRealm, String recordOwner){
        
        Realm emptyRealm = new Realm("empty");
        Realm theRealm = realms.getOrDefault(recordRealm, emptyRealm);
        Realm magicRealm = realms.getOrDefault("*", emptyRealm);
        return hasAccess(userName, userRoles, theRealm.getWritingRoles(), magicRealm.getWritingRoles(), recordOwner);
        
    }
    
    public boolean canWriteToAnyRealm(Authentication auth){
        Collection<String> userRoles = new HashSet<>();
        for(GrantedAuthority ga : auth.getAuthorities()){
            userRoles.add(ga.getAuthority());
        }
        return canWriteToAnyRealm(userRoles);
    }
    
    public boolean canWriteToAnyRealm(Collection<String> userRoles){
        Realm emptyRealm = new Realm("empty");
        Realm magicRealm = realms.getOrDefault("*", emptyRealm);
        return hasAccessToAllRealms(userRoles, magicRealm.getWritingRoles());
    }
    
    private boolean hasAccess(String userName, Collection<String> userRoles, Collection<String> realmAccessRoles, 
        Collection<String> magicAccessRoles, String recordOwner){
        
        //Owner of the record can access it
        if (StringUtils.hasText(userName) && StringUtils.hasText(recordOwner) && userName.equals(recordOwner)){
            return true;
        }
        
        //If the realm allows access to * (everybody)
        if (realmAccessRoles != null && realmAccessRoles.contains("*")){
            return true;
        }
        
        for(String role : userRoles){
            
            if (magicAccessRoles != null && (magicAccessRoles.contains("*") || magicAccessRoles.contains(role))){
                //If the * (every realm) allows access to this role
                return true;
            }else if (realmAccessRoles != null && realmAccessRoles.contains(role)){
                //if the realm has this role in its list
                return true;
            }
            
        }
        return false;
    }
    
    private boolean hasAccessToAllRealms(Collection<String> userRoles, Collection<String> magicAccessRoles){
                
        for(String role : userRoles){
            if (magicAccessRoles != null && (magicAccessRoles.contains("*") || magicAccessRoles.contains(role))){
                //If the * (every realm) allows access to this role
                return true;
            }
        }
        return false;
    }
    
    public void addAccess(Realm realm){
        if (!this.realms.containsKey(realm.getName())){
            this.realms.put(realm.getName(), realm);
        }else{
            Realm theRealm = this.realms.get(realm.getName());
            for(String readingRole : realm.getReadingRoles()){
                theRealm.addReadingRoles(readingRole);
            }
            for(String writingRole : realm.getWritingRoles()){
                theRealm.addWritingRoles(writingRole);
            }
        } 
    }
    
    public void addAccess(Collection<Realm> realmsToAdd){
        for(Realm realm : realmsToAdd){
            addAccess(realm);
        }
    }
    
    public void replaceAllRealms(Collection<Realm> newRealms){
        Collection<String> newRealmNames = new HashSet<>();
        newRealms.stream().forEach((realm) -> {
            newRealmNames.add(realm.getName());
            this.realms.put(realm.getName(), realm);
        });
        Collection<String> realmsToRemove = new HashSet<>();
        for(Map.Entry<String, Realm> realmEntry : this.realms.entrySet()){
            if (!newRealmNames.contains(realmEntry.getKey())){
                realmsToRemove.add(realmEntry.getKey());
            }
        }
        for(String realmName : realmsToRemove){
            this.realms.remove(realmName);
        }
    }
    
    public void replaceRealm(Realm realmToReplace){
        this.realms.put(realmToReplace.getName(), realmToReplace);
    }
    
}
