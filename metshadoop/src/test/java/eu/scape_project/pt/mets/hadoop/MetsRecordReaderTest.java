/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.mets.hadoop;

import eu.scape_project.pt.mets.hadoop.DTO;
import eu.scape_project.pt.mets.hadoop.MetsRecordReader;
import eu.scape_project.pt.mets.hadoop.MetsInputFormat;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.util.ScapeMarshaller;
import gov.loc.mets.MetsType;
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
import sun.util.LocaleServiceProviderPool;

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
        DTO.setType(String.class);

        System.out.println("DTO.type = " + DTO.type.getName());

        Configuration conf = new Configuration();
        conf.set(MetsInputFormat.TAG, "mets:mets");
        Path path = new Path("tmp" + System.currentTimeMillis() );
        FileSystem fs = path.getFileSystem(conf);

        String xmlFile = DTO.type.equals(MetsType.class) ?
                    "metsdocs.xml" : 
                    ( DTO.type.equals(IntellectualEntity.class) ? 
                    "entities.xml" : "entities.xml");

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
    public void testNextKeyValueIntellectualEntity() throws Exception {
        System.out.println("TEST: nextKeyValue IntellectualEntity");
        DTO.setType(IntellectualEntity.class);
        String tag = context.getConfiguration().get(MetsInputFormat.TAG);
        MetsRecordReader instance = new MetsRecordReader(tag);
        instance.initialize(genericSplit, context);
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
    }

    /**
     * Test of nextKeyValue method, of class MetsRecordReader.
     */
    @Ignore 
    public void testNextKeyValueMetsType() throws Exception {
        System.out.println("TEST: nextKeyValue MetsType");
        DTO.setType(MetsType.class);
        String tag = context.getConfiguration().get(MetsInputFormat.TAG);
        MetsRecordReader instance = new MetsRecordReader(tag);
        instance.initialize(genericSplit, context);
        for( int i = 1; i <= 2; i++ ) {
            instance.nextKeyValue();
            InputStream in = this.getClass().getClassLoader()
                    .getResourceAsStream("metsDoc"+i+".xml");

            MetsType doc = (MetsType) ScapeMarshaller.newInstance().deserialize(in);
            MetsType value = (MetsType) instance.getCurrentValue().getObject();

            assertEquals(doc.getID(), value.getID());
        }
    }
}
