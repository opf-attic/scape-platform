/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.mets.hadoop.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Matthias Rella, AIT-DME
 */
public class XmlEntityTest {
    
    public XmlEntityTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of get method, of class XmlEntity.
     */
    @Test
    public void testGet() throws IOException {
        System.out.println("get");
        
        InputStream in = this.getClass().getClassLoader()
                .getResourceAsStream("entity1.xml");
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(in, baos);
        XmlEntity instance = new XmlEntity(baos.toString());

        String expResult = "a test entity";
        String result = instance.get("dc:title");
        System.out.println("result = " + result + ", expResult = " + expResult);
        assertEquals(expResult, result);

        expResult = "6448b8bd-b683-4754-b668-eba2f6db29b3";
        result = instance.get("mets:mets@ID");
        System.out.println("result = " + result + ", expResult = " + expResult);
        assertEquals(expResult, result);
    }

    /**
     * Test of set method, of class XmlEntity.
     */
    @Test
    public void testSet() throws IOException {
        System.out.println("set");
        InputStream in = this.getClass().getClassLoader()
                .getResourceAsStream("entity1.xml");
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(in, baos);
        XmlEntity instance = new XmlEntity(baos.toString());
        String newContent = "a new title for the testentity";
        instance.set("dc:title", newContent);
        String result = instance.get("dc:title");
        String expResult = newContent;
        System.out.println("result = " + result + ", expResult = " + expResult);
        assertEquals(expResult, result);
    }
}