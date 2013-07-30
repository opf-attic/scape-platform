/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.mets.hadoop;

import java.io.*;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;

/**
 *
 * @author ait
 */
public class DTOTest {
    
    public DTOTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     * Test of getIdentifier method, of class DTO.
     */
    @Test
    public void testGetIdentifier() throws IOException {
        DTO.setType(String.class);
        DTO dto = new DTO();
        InputStream in = this.getClass().getClassLoader()
                .getResourceAsStream("metsDoc1.xml");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(in, baos);
        dto.setObject(baos.toString());

        assertEquals("DMD-82786f1e-fed9-4426-9c4d-4867f179d3c5", 
                dto.getIdentifier());

        in = this.getClass().getClassLoader()
                .getResourceAsStream("entity1.xml");
        baos = new ByteArrayOutputStream();
        IOUtils.copy(in, baos);
        dto.setObject(baos.toString());

        assertEquals("6448b8bd-b683-4754-b668-eba2f6db29b3", 
                dto.getIdentifier());
    }

}
