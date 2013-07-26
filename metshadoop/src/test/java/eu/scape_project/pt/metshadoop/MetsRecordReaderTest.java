/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.metshadoop;

import eu.scape_project.pt.mets.hadoop.DTO;
import eu.scape_project.pt.mets.hadoop.MetsRecordReader;
import eu.scape_project.pt.mets.hadoop.MetsInputFormat;
import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.util.ScapeMarshaller;
import java.io.InputStream;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import static org.junit.Assert.*;
import org.junit.*;

public class MetsRecordReaderTest{

    String beginning, ending, rawData1, rawData2, startTag, rootAttr;
    InputSplit genericSplit;
    TaskAttemptContext context;
    
    public MetsRecordReaderTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        DTO.setType(MetsDocument.class);

        System.out.println("DTO.type = " + DTO.type.getName());

        Configuration conf = new Configuration();
        conf.set(MetsInputFormat.TAG, "mets:mets");
        Path path = new Path("tmp" + System.currentTimeMillis() );
        FileSystem fs = path.getFileSystem(conf);

        String xmlFile = DTO.type.equals(MetsDocument.class) ?
                    "metsdocs.xml" : 
                    ( DTO.type.equals(IntellectualEntity.class) ? 
                    "entities.xml" : null);

        URL res = this.getClass().getClassLoader().getResource(xmlFile);
        fs.copyFromLocalFile(new Path(res.toURI()), path );
        FileStatus stat = fs.getFileStatus(path);
        genericSplit = new FileSplit(path, 0, stat.getLen(), null);
        context = new TaskAttemptContext(conf, new TaskAttemptID());
    }
    
    @After
    public void tearDown() throws Exception {
        FileSplit split = (FileSplit)genericSplit;
        Configuration conf = context.getConfiguration();

        FileSystem fs = split.getPath().getFileSystem(conf);
        fs.delete(split.getPath(), true);
    }

    /**
     * Test of getProgress method, of class MetsRecordReader.
     */
    @Test
    public void testGetProgress() throws Exception {
        System.out.println("TEST: getProgress");
        String tag = context.getConfiguration().get(MetsInputFormat.TAG);
        MetsRecordReader instance = new MetsRecordReader(tag);
        instance.initialize(genericSplit, context);
        float expResult = 0.0F;
        System.out.println("nextKeyValue");
        instance.nextKeyValue();
        float result = instance.getProgress();
        System.out.println("progress = " + result );
        assertTrue(result < 1 );
        System.out.println("nextKeyValue");
        instance.nextKeyValue();
        result = instance.getProgress();
        System.out.println("progress = " + result );
        System.out.println("nextKeyValue");
        instance.nextKeyValue();
        result = instance.getProgress();
        System.out.println("progress = " + result );
        assertTrue(result == 1 );
    }

    /**
     * Test of nextKeyValue method, of class MetsRecordReader.
     */
    @Test 
    public void testNextKeyValue() throws Exception {
        System.out.println("TEST: nextKeyValue");
        String tag = context.getConfiguration().get(MetsInputFormat.TAG);
        MetsRecordReader instance = new MetsRecordReader(tag);
        instance.initialize(genericSplit, context);
        if( DTO.type.equals(MetsDocument.class)) {
            for( int i = 1; i <= 2; i++ ) {
                instance.nextKeyValue();
                InputStream in = this.getClass().getClassLoader()
                        .getResourceAsStream("metsDoc"+i+".xml");

                MetsDocument doc = (MetsDocument) ScapeMarshaller.newInstance().getJaxbUnmarshaller().unmarshal(in);
                MetsDocument value = (MetsDocument) instance.getCurrentValue().getObject();
                assertEquals(doc.getId(), value.getId());
                assertEquals(doc.getObjId(), value.getObjId());
                assertEquals(doc.getLabel(), value.getLabel());
                assertEquals(doc.getHeaders().get(0).getId(), value.getHeaders().get(0).getId());
                assertEquals(doc.getHeaders().get(0).getRecordStatus(), value.getHeaders().get(0).getRecordStatus());
                assertEquals(doc.getHeaders().get(0).getVersionNumber(), value.getHeaders().get(0).getVersionNumber());

                assertEquals(doc.getDmdSec().getId(), value.getDmdSec().getId());
                assertEquals(doc.getDmdSec().getMetadataReference().getId(), value.getDmdSec().getMetadataReference().getId());
                assertEquals(doc.getDmdSec().getMetadataReference().getSize(), value.getDmdSec().getMetadataReference().getSize());
                assertEquals(doc.getDmdSec().getMetadataReference().getHref(), value.getDmdSec().getMetadataReference().getHref());
                assertEquals(doc.getFileSec().getId(), value.getFileSec().getId());
                assertEquals(doc.getStructMaps().get(0).getDivisions().get(0).getType(), value.getStructMaps().get(0).getDivisions().get(0).getType());
                assertEquals(doc.getStructMaps().get(0).getDivisions().get(0).getOrder(), value.getStructMaps().get(0).getDivisions().get(0).getOrder());
            }
        } else if (DTO.type.equals(IntellectualEntity.class)){
            for( int i = 1; i <= 2; i++ ) {
                instance.nextKeyValue();
                InputStream in = this.getClass().getClassLoader()
                        .getResourceAsStream("entity"+i+".xml");

                IntellectualEntity entity = (IntellectualEntity) ScapeMarshaller.newInstance().deserialize(IntellectualEntity.class, in);
                IntellectualEntity value = (IntellectualEntity) instance.getCurrentValue().getObject();
                assertEquals(entity.getIdentifier().getValue(), value.getIdentifier().getValue());
                assertEquals(entity.getVersionNumber(), value.getVersionNumber());
                //assertEquals(((DCMetadata)entity.getDescriptive()).getDate(), ((DCMetadata)value.getDescriptive()).getDate());
                //assertEquals(((DCMetadata)entity.getDescriptive()).getTitle(), ((DCMetadata)value.getDescriptive()).getTitle());
                //assertEquals(((DCMetadata)entity.getDescriptive()).getLanguage(), ((DCMetadata)value.getDescriptive()).getLanguage());

            }

        } else {
            fail("Current set DTO.type not testable");
        }
        //String value = startTag + rootAttr + " " + rawData1;
        //assertEquals(value, instance.getRawData());
        //fail("The test case is a prototype.");
    }
}
