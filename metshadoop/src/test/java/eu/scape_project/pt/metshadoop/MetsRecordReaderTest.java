/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.metshadoop;

import eu.scape_project.pt.metshadoop.MetsRecordReader;
import eu.scape_project.pt.metshadoop.DTO;
import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.mets.SCAPEMarshaller;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Ignore;
import org.junit.Test;

public class MetsRecordReaderTest extends TestCase {

    String beginning, ending, rawData1, rawData2, startTag, rootAttr;
    InputSplit genericSplit;
    TaskAttemptContext context;
    
    public MetsRecordReaderTest(String testName) {
        super(testName);
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Configuration conf = new Configuration();
        conf.set(MetsRecordReader.TAG, "mets:mets");
        Path path = new Path("tmp" + System.currentTimeMillis() );
        FileSystem fs = path.getFileSystem(conf);
        URL res = this.getClass().getClassLoader().getResource("entityList.xml");
        fs.copyFromLocalFile(new Path(res.toURI()), path );
        FileStatus stat = fs.getFileStatus(path);
        genericSplit = new FileSplit(path, 0, stat.getLen(), null);
        context = new TaskAttemptContext(conf, new TaskAttemptID());
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
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
        MetsRecordReader instance = new MetsRecordReader();
        instance.initialize(genericSplit, context);
        float expResult = 0.0F;
        instance.nextKeyValue();
        float result = instance.getProgress();
        System.out.println("result = " + result );
        assertTrue(result < 1 );
        instance.nextKeyValue();
        result = instance.getProgress();
        System.out.println("result = " + result );
        instance.nextKeyValue();
        result = instance.getProgress();
        System.out.println("result = " + result );
        assertTrue(result == 1 );
        //assertEquals(expResult, result, 0.0);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of nextKeyValue method, of class MetsRecordReader.
     */
    @Test 
    public void testNextKeyValue() throws Exception {
        System.out.println("TEST: nextKeyValue");
        MetsRecordReader instance = new MetsRecordReader();
        instance.initialize(genericSplit, context);
        if( DTO.type.equals(MetsDocument.class)) {
            for( int i = 1; i <= 2; i++ ) {
                instance.nextKeyValue();
                InputStream in = this.getClass().getClassLoader()
                        .getResourceAsStream("metsDoc"+i+".xml");

                MetsDocument doc = (MetsDocument) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(in);
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
        } else {
            fail("Current set DTO.type not testable");
        }
        //String value = startTag + rootAttr + " " + rawData1;
        //assertEquals(value, instance.getRawData());
        //fail("The test case is a prototype.");
    }
}
