/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.metshadoop;

import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.metadata.dc.DCMetadata;
import eu.scapeproject.model.mets.SCAPEMarshaller;
import java.io.InputStream;
import java.net.URL;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

public class MetsRecordReaderWithRealMetsDocTest extends TestCase {

    String beginning, ending, rawData1, rawData2, startTag, rootAttr;
    InputSplit genericSplit;
    TaskAttemptContext context;
    
    public MetsRecordReaderWithRealMetsDocTest(String testName) {
        super(testName);
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        DTO.setType(MetsDocument.class);

        System.out.println("DTO.type = " + DTO.type.getName());

        Configuration conf = new Configuration();
        conf.set(MetsInputFormat.TAG, "mets:mets");
        Path path = new Path("tmp" + System.currentTimeMillis() );
        FileSystem fs = path.getFileSystem(conf);

        String xmlFile = "read-metsDocs.xml"; 

        URL res = this.getClass().getClassLoader().getResource(xmlFile);
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
     * Test of nextKeyValue method, of class MetsRecordReader.
     */
    @Test 
    public void testNextKeyValue() throws Exception {
        System.out.println("TEST: nextKeyValue");
        String tag = context.getConfiguration().get(MetsInputFormat.TAG);
        MetsRecordReader instance = new MetsRecordReader(tag);
        instance.initialize(genericSplit, context);
        instance.nextKeyValue();
        InputStream in = this.getClass().getClassLoader()
                .getResourceAsStream("real-metsDoc1.xml");

        MetsDocument doc = (MetsDocument) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(in);
        MetsDocument value = (MetsDocument) instance.getCurrentValue().getObject();
        assertEquals(doc.getId(), value.getId());
        assertEquals(doc.getObjId(), value.getObjId());
        assertEquals(doc.getLabel(), value.getLabel());
        assertEquals(doc.getHeaders().get(0).getId(), value.getHeaders().get(0).getId());
        assertEquals(doc.getHeaders().get(0).getRecordStatus(), value.getHeaders().get(0).getRecordStatus());
        assertEquals(doc.getHeaders().get(0).getVersionNumber(), value.getHeaders().get(0).getVersionNumber());

        assertEquals(doc.getDmdSec().getId(), value.getDmdSec().getId());
        if( doc.getDmdSec().getMetadataReference() != null ) {
            assertEquals(doc.getDmdSec().getMetadataReference().getId(), value.getDmdSec().getMetadataReference().getId());
            assertEquals(doc.getDmdSec().getMetadataReference().getSize(), value.getDmdSec().getMetadataReference().getSize());
            assertEquals(doc.getDmdSec().getMetadataReference().getHref(), value.getDmdSec().getMetadataReference().getHref());
        }
        assertEquals(doc.getFileSec().getId(), value.getFileSec().getId());
        assertEquals(doc.getStructMaps().get(0).getDivisions().get(0).getType(), value.getStructMaps().get(0).getDivisions().get(0).getType());
        assertEquals(doc.getStructMaps().get(0).getDivisions().get(0).getOrder(), value.getStructMaps().get(0).getDivisions().get(0).getOrder());
        //String value = startTag + rootAttr + " " + rawData1;
        //assertEquals(value, instance.getRawData());
        //fail("The test case is a prototype.");
    }
}
