package org.datakow.security.access;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

/**
 *
 * @author kevin.off
 */
public class AccessManagerTest {
    
    private AccessManager accessManager;
    
    public AccessManagerTest() {
    }

    @Before
    public void setup(){
        accessManager = new AccessManager();
        accessManager.addAccess(new Realm("*", new Read("ROLE_ADMIN"), new Write("ROLE_ADMIN")));
        accessManager.addAccess(new Realm("secret", new Read("ROLE_SECRET"), new Write("ROLE_SECRET")));
        accessManager.addAccess(new Realm("*", new Read("ROLE_BANANA"), new Write("ROLE_BANANA")));
        accessManager.addAccess(new Realm("*", new Read("ROLE_READER"), new Write()));
        accessManager.addAccess(new Realm("banana", new Read("ROLE_BANANA"), new Write("ROLE_BANANA","ROLE_MONKEY")));
        accessManager.addAccess(new Realm("internal", new Read("*"), new Write()));
        accessManager.addAccess(new Realm("public", new Read("*"), new Write()));
        accessManager.addAccess(new Realm("public", new Read(), new Write("*")));
        accessManager.addAccess(new Realm("", new Read("*"), new Write("*")));
        accessManager.addAccess(new Realm(null, new Read("*"), new Write("*")));
    }


    @Test
    public void testGetReadingRoles() {
        Collection<String> readingRoles = accessManager.getReadingRoles("someRealm");
        assertArrayEquals(Arrays.asList("ROLE_READER", "ROLE_ADMIN", "ROLE_BANANA").toArray(), readingRoles.toArray());
        readingRoles = accessManager.getReadingRoles("secret");
        assertArrayEquals(Arrays.asList("ROLE_SECRET", "ROLE_READER", "ROLE_ADMIN", "ROLE_BANANA").toArray(), readingRoles.toArray());
        readingRoles = accessManager.getReadingRoles("internal");
        assertArrayEquals(Arrays.asList("*").toArray(), readingRoles.toArray());
        readingRoles = accessManager.getReadingRoles("public");
        assertArrayEquals(Arrays.asList("*").toArray(), readingRoles.toArray());
    }

    @Test
    public void testGetWritingRoles() {
        
        Collection<String> readingRoles = accessManager.getWritingRoles("some_role");
        assertArrayEquals(Arrays.asList("ROLE_ADMIN", "ROLE_BANANA").toArray(), readingRoles.toArray());
        readingRoles = accessManager.getWritingRoles("secret");
        assertArrayEquals(Arrays.asList("ROLE_SECRET", "ROLE_ADMIN", "ROLE_BANANA").toArray(), readingRoles.toArray());
        readingRoles = accessManager.getReadingRoles("internal");
        assertArrayEquals(Arrays.asList("*").toArray(), readingRoles.toArray());
        readingRoles = accessManager.getReadingRoles("public");
        assertArrayEquals(Arrays.asList("*").toArray(), readingRoles.toArray());
        
    }

    @Test
    public void testCanRead_3args() {
        SimpleGrantedAuthority admin = new SimpleGrantedAuthority("ROLE_ADMIN");
        SimpleGrantedAuthority secret = new SimpleGrantedAuthority("ROLE_SECRET");
        SimpleGrantedAuthority banana = new SimpleGrantedAuthority("ROLE_BANANA");
        SimpleGrantedAuthority none = new SimpleGrantedAuthority("ROLE_NONE");
        
        UsernamePasswordAuthenticationToken adminuser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(admin));
        UsernamePasswordAuthenticationToken secretuser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(secret));
        UsernamePasswordAuthenticationToken bananauser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(banana));
        UsernamePasswordAuthenticationToken noneuser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(none));
        UsernamePasswordAuthenticationToken nulluser = new UsernamePasswordAuthenticationToken("somebody", null, null);
        
        assertTrue(accessManager.canRead(adminuser, "secret", "somebodyElse"));
        assertTrue(accessManager.canRead(adminuser, "internal", "somebodyElse"));
        assertTrue(accessManager.canRead(adminuser, null, "somebodyElse"));
        
        assertTrue(accessManager.canRead(secretuser, "secret", "somebodyElse"));
        assertTrue(accessManager.canRead(bananauser, "secret", "somebodyElse"));
        assertTrue(accessManager.canRead(bananauser, "topSecret", "somebodyElse"));
        
        assertTrue(accessManager.canRead(noneuser, "secret", "somebody"));
        assertTrue(accessManager.canRead(noneuser, "topSecret", "somebody"));
        assertTrue(accessManager.canRead(nulluser, "topSecret", "somebody"));
        
        assertTrue(accessManager.canRead(secretuser, "internal", "somebodyElse"));
        assertTrue(accessManager.canRead(nulluser, "internal", "somebodyElse"));
        
        assertTrue(accessManager.canRead(secretuser, "public", "somebodyElse"));
        assertTrue(accessManager.canRead(secretuser, "", "somebodyElse"));
        assertTrue(accessManager.canRead(secretuser, null, "somebodyElse"));
        
        assertTrue(accessManager.canRead(nulluser, "public", "somebodyElse"));
        assertTrue(accessManager.canRead(nulluser, "", "somebodyElse"));
        assertTrue(accessManager.canRead(nulluser, null, "somebodyElse"));
        
        assertFalse(accessManager.canRead(noneuser, "secret", "somebodyElse"));
        assertFalse(accessManager.canRead(noneuser, "topSecret", "somebodyElse"));
        
        assertFalse(accessManager.canRead(noneuser, "secret", "somebodyElse"));
        assertFalse(accessManager.canRead(noneuser, "topSecret", "somebodyElse"));
        assertFalse(accessManager.canRead(nulluser, "topSecret", "somebodyElse"));
    }

    @Test
    public void testCanRead_4args() {
        
        //Is Admin
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_ADMIN"), "secret", "somebodyelse"));
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_ADMIN", "ROLE_NONE"), "secret", "somebodyelse"));
        //Is Admin
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_ADMIN"), "internal", "somebodyelse"));
        //Is Admin
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_ADMIN"), null, "somebodyelse"));
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_NONE", "ROLE_ADMIN"), null, "somebodyelse"));
        
        //has role
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_SECRET"), "secret", "somebodyelse"));
        //can read anything
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_BANANA"), "secret", "somebodyelse"));
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_BANANA", "ROLE_SECRET"), "secret", "somebodyelse"));
        //can read anything even if the realm is not listed
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_BANANA"), "topSecret", "somebodyelse"));
        
        //is the owner
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_NONE"), "secret", "somebody"));
        //is the owner and realm is not listed
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_NONE"), "topSecret", "somebody"));
        //is the owner and realm is not listed
        assertTrue(accessManager.canRead("somebody", null, "topSecret", "somebody"));
        
        //Everybody can read internal
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_SECRET"), "internal", "somebodyelse"));
        //Everybody can read internal even if you don't have a role at all
        assertTrue(accessManager.canRead("somebody", null, "internal", "somebodyelse"));
        
        //Everybody can read public
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_SECRET"), "public", "somebodyelse"));
        //Everybody can read ""
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_SECRET"), "", "somebodyelse"));
        //Everybody can read null
        assertTrue(accessManager.canRead("somebody", Arrays.asList("ROLE_SECRET"), null, "somebodyelse"));
        
        //Everybody can read public
        assertTrue(accessManager.canRead("somebody", null, "public", "somebodyelse"));
        //Everybody can read ""
        assertTrue(accessManager.canRead("somebody", null, "", "somebodyelse"));
        //Everybody can read null
        assertTrue(accessManager.canRead("somebody", null, null, "somebodyelse"));
        
        //has role
        assertFalse(accessManager.canRead("somebody", Arrays.asList("ROLE_NOBODY"), "secret", "somebodyelse"));
        assertFalse(accessManager.canRead("somebody", Arrays.asList("ROLE_NOBODY", "*"), "secret", "somebodyelse"));
        //can read anything even if the realm is not listed
        assertFalse(accessManager.canRead("somebody", Arrays.asList("ROLE_NOBODY"), "topSecret", "somebodyelse"));
        
        //is not the owner
        assertFalse(accessManager.canRead("somebody", Arrays.asList("ROLE_NONE"), "secret", "somebodyelse"));
        //is not the owner and realm is not listed
        assertFalse(accessManager.canRead("somebody", Arrays.asList("ROLE_NONE"), "topSecret", "somebodyelse"));
        //is not the owner and realm is not listed
        assertFalse(accessManager.canRead("somebody", new ArrayList<>(), "topSecret", "somebodyelse"));
        
    }

    @Test
    public void testCanWrite_3args() {
        
        SimpleGrantedAuthority admin = new SimpleGrantedAuthority("ROLE_ADMIN");
        SimpleGrantedAuthority secret = new SimpleGrantedAuthority("ROLE_SECRET");
        SimpleGrantedAuthority banana = new SimpleGrantedAuthority("ROLE_BANANA");
        SimpleGrantedAuthority none = new SimpleGrantedAuthority("ROLE_NONE");
        
        UsernamePasswordAuthenticationToken adminuser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(admin));
        UsernamePasswordAuthenticationToken secretuser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(secret));
        UsernamePasswordAuthenticationToken bananauser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(banana));
        UsernamePasswordAuthenticationToken noneuser = new UsernamePasswordAuthenticationToken("somebody", null, Arrays.asList(none));
        UsernamePasswordAuthenticationToken nulluser = new UsernamePasswordAuthenticationToken("somebody", null, null);
        
        assertTrue(accessManager.canWrite(adminuser, "secret", "somebodyElse"));
        assertTrue(accessManager.canWrite(adminuser, "internal", "somebodyElse"));
        assertTrue(accessManager.canWrite(adminuser, null, "somebodyElse"));
        
        assertTrue(accessManager.canWrite(secretuser, "secret", "somebodyElse"));
        assertTrue(accessManager.canWrite(bananauser, "secret", "somebodyElse"));
        assertTrue(accessManager.canWrite(bananauser, "topSecret", "somebodyElse"));
        
        assertTrue(accessManager.canWrite(noneuser, "secret", "somebody"));
        assertTrue(accessManager.canWrite(noneuser, "topSecret", "somebody"));
        assertTrue(accessManager.canWrite(nulluser, "topSecret", "somebody"));
        
        assertFalse(accessManager.canWrite(secretuser, "internal", "somebodyElse"));
        assertFalse(accessManager.canWrite(nulluser, "internal", "somebodyElse"));
        
        assertTrue(accessManager.canWrite(secretuser, "public", "somebodyElse"));
        assertTrue(accessManager.canWrite(secretuser, "", "somebodyElse"));
        assertTrue(accessManager.canWrite(secretuser, null, "somebodyElse"));
        
        assertTrue(accessManager.canWrite(nulluser, "public", "somebodyElse"));
        assertTrue(accessManager.canWrite(nulluser, "", "somebodyElse"));
        assertTrue(accessManager.canWrite(nulluser, null, "somebodyElse"));
        
        assertFalse(accessManager.canWrite(noneuser, "secret", "somebodyElse"));
        assertFalse(accessManager.canWrite(noneuser, "topSecret", "somebodyElse"));
        
        assertFalse(accessManager.canWrite(noneuser, "secret", "somebodyElse"));
        assertFalse(accessManager.canWrite(noneuser, "topSecret", "somebodyElse"));
        assertFalse(accessManager.canWrite(nulluser, "topSecret", "somebodyElse"));
        
        assertTrue(accessManager.canWrite(adminuser, null, null));
        assertTrue(accessManager.canWrite(adminuser, "", null));
        assertTrue(accessManager.canWrite(adminuser, null, ""));
        assertTrue(accessManager.canWrite(adminuser, "", ""));
        
    }

    @Test
    public void testCanWrite_4args() {
        
        //Is Admin
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_ADMIN"), "secret", "somebodyelse"));
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_ADMIN", "ROLE_NONE"), "secret", "somebodyelse"));
        //Is Admin
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_ADMIN"), "internal", "somebodyelse"));
        //Is Admin
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_ADMIN"), null, "somebodyelse"));
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_NONE", "ROLE_ADMIN"), null, "somebodyelse"));
        
        //has role
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_SECRET"), "secret", "somebodyelse"));
        //can read anything
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_BANANA"), "secret", "somebodyelse"));
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_BANANA", "ROLE_SECRET"), "secret", "somebodyelse"));
        //can read anything even if the realm is not listed
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_BANANA"), "topSecret", "somebodyelse"));
        
        //is the owner
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_NONE"), "secret", "somebody"));
        //is the owner and realm is not listed
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_NONE"), "topSecret", "somebody"));
        //is the owner and realm is not listed
        assertTrue(accessManager.canWrite("somebody", new ArrayList<>(), "topSecret", "somebody"));
        
        //Everybody can read internal
        assertFalse(accessManager.canWrite("somebody", Arrays.asList("ROLE_SECRET"), "internal", "somebodyelse"));
        //Everybody can read internal even if you don't have a role at all
        assertFalse(accessManager.canWrite("somebody", new ArrayList<>(), "internal", "somebodyelse"));
        
        //Everybody can read public
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_SECRET"), "public", "somebodyelse"));
        //Everybody can read ""
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_SECRET"), "", "somebodyelse"));
        //Everybody can read null
        assertTrue(accessManager.canWrite("somebody", Arrays.asList("ROLE_SECRET"), null, "somebodyelse"));
        
        //Everybody can read public
        assertTrue(accessManager.canWrite("somebody", new ArrayList<>(), "public", "somebodyelse"));
        //Everybody can read ""
        assertTrue(accessManager.canWrite("somebody", new ArrayList<>(), "", "somebodyelse"));
        //Everybody can read null
        assertTrue(accessManager.canWrite("somebody", new ArrayList<>(), null, "somebodyelse"));
        
        //has role
        assertFalse(accessManager.canWrite("somebody", Arrays.asList("ROLE_NOBODY"), "secret", "somebodyelse"));
        assertFalse(accessManager.canWrite("somebody", Arrays.asList("ROLE_NOBODY", "*"), "secret", "somebodyelse"));
        //can read anything even if the realm is not listed
        assertFalse(accessManager.canWrite("somebody", Arrays.asList("ROLE_NOBODY"), "topSecret", "somebodyelse"));
        
        //is not the owner
        assertFalse(accessManager.canWrite("somebody", Arrays.asList("ROLE_NONE"), "secret", "somebodyelse"));
        //is not the owner and realm is not listed
        assertFalse(accessManager.canWrite("somebody", Arrays.asList("ROLE_NONE"), "topSecret", "somebodyelse"));
        //is not the owner and realm is not listed
        assertFalse(accessManager.canWrite("somebody", new ArrayList<>(), "topSecret", "somebodyelse"));
        
    }

    @Test
    public void testAddAccess_Realm() {
        
        accessManager.addAccess(new Realm("secret", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")));
        Collection<String> supermanRealms = accessManager.getWritingRealms("ROLE_SUPERMAN");
        assertArrayEquals(Arrays.asList("", null, "public", "secret").toArray(), supermanRealms.toArray());
        
        accessManager.addAccess(new Realm("secret", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")));
        accessManager.addAccess(new Realm("super", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")));
        supermanRealms = accessManager.getWritingRealms("ROLE_SUPERMAN");
        assertArrayEquals(Arrays.asList("", null, "super", "public", "secret").toArray(), supermanRealms.toArray());
        
    }

    @Test
    public void testAddAccess_List() {
        
        accessManager.addAccess(Arrays.asList(new Realm("secret", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")),
                new Realm("super", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN"))));
        Collection<String> supermanWritingRealms = accessManager.getWritingRealms("ROLE_SUPERMAN");
        Collection<String> secretReaderReadingRealms = accessManager.getReadingRealms("ROLE_SECRET_READER");
        assertArrayEquals(Arrays.asList("", null, "super", "public", "secret").toArray(), supermanWritingRealms.toArray());
        assertArrayEquals(Arrays.asList("", null, "super", "internal", "public", "secret").toArray(), secretReaderReadingRealms.toArray());
        
    }

    @Test
    public void testReplaceAllRealms() {
        
        accessManager.addAccess(Arrays.asList(
                new Realm("secret", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")),
                new Realm("super", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN"))));

        
        List<Realm> realms = new ArrayList<>();
        realms.add(new Realm("new", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")));
        realms.add(new Realm("anotherNew", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")));
        
        accessManager.replaceAllRealms(realms);
        
        assertFalse(accessManager.canRead("me", Arrays.asList("ROLE_ADMIN"), "someRealm", "notMe"));
        assertTrue(accessManager.canRead("me", Arrays.asList("ROLE_SECRET_READER"), "new", "notMe"));
        
    }

    @Test
    public void testReplaceRealm() {
        
        
        accessManager.replaceRealm(new Realm("secret", new Read("ROLE_SECRET_READER"), new Write("ROLE_ALL_WRITER", "ROLE_SUPERMAN")));
        assertTrue(accessManager.canRead("me", Arrays.asList("ROLE_SECRET_READER"), "secret", "notMe"));
        assertFalse(accessManager.canRead("me", Arrays.asList("ROLE_SECRET"), "secret", "notMe"));
        
    }

    @Test
    public void testGetReadingRealms_Authentication() {
    }

    @Test
    public void testGetReadingRealms_String() {
        Collection<String> secretRealms = accessManager.getReadingRealms("ROLE_SECRET");
        Collection<String> bananaRealms = accessManager.getReadingRealms("ROLE_BANANA");
        assertArrayEquals(Arrays.asList("", null, "internal", "public", "secret").toArray(), secretRealms.toArray());
        assertArrayEquals(Arrays.asList("*").toArray(), bananaRealms.toArray());
    }

    @Test
    public void testGetWritingRealms_Authentication() {
    }

    @Test
    public void testGetWritingRealms_String() {
        Collection<String> secretRealms = accessManager.getWritingRealms("ROLE_SECRET");
        Collection<String> bananaRealms = accessManager.getWritingRealms("ROLE_BANANA");
        assertArrayEquals(Arrays.asList("", null, "public", "secret").toArray(), secretRealms.toArray());
        assertArrayEquals(Arrays.asList("*").toArray(), bananaRealms.toArray());
    }

    

    @Test
    public void testCanReadFromAnyRealm() {
        assertTrue(accessManager.canReadFromAnyRealm(Arrays.asList("ROLE_ADMIN")));
        assertTrue(accessManager.canReadFromAnyRealm(Arrays.asList("ROLE_READER")));
        assertFalse(accessManager.canReadFromAnyRealm(Arrays.asList("ROLE_SECRET")));
        assertFalse(accessManager.canReadFromAnyRealm(Arrays.asList("asdf")));
    }

    @Test
    public void testCanWriteToAnyRealm_Authentication() {
        assertTrue(accessManager.canWriteToAnyRealm(Arrays.asList("ROLE_ADMIN")));
        assertFalse(accessManager.canWriteToAnyRealm(Arrays.asList("ROLE_SECRET")));
        assertFalse(accessManager.canWriteToAnyRealm(Arrays.asList("asdf")));
    }

    @Test
    public void testCanWriteToAnyRealm_Collection() {
    }

    @Test
    public void testAddAccess_Collection() {
    }
    
}
