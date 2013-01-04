package eu.scape_project.pt.metshadoop;

import eu.scape_project.pt.metshadoop.utils.XmlUtil;
import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.IntellectualEntityCollection;
import eu.scapeproject.model.mets.SCAPEMarshaller;
import eu.scapeproject.model.util.MetsUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes DTOs to an FSDataOutputStream.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class MetsRecordWriter extends RecordWriter<Text, DTO> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MetsRecordWriter.class);

    FSDataOutputStream out;

    /**
     * Tag name of record
     */
    private final String tag;
    private boolean isFirst = true;
    private XmlUtil xml;

    public MetsRecordWriter(FSDataOutputStream fileOut, String tag ) {
        out = fileOut;
        this.tag = tag;
    }

    /**
     * Writes the closing root tag to the output stream. 
     * 
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        xml.reset();
        xml.readDeclaration();
        xml.readRootTag();
        xml.writeClosingRootTag(out);
        out.close();


    }

    /**
     * Writes a DTO to the output stream. Marshals the DTO as a one-item list,
     * and extracts the XML of the DTO from the result. Then writes it to the 
     * stream. On the first marshalling (the first DTO) the xml declaration and
     * the root tag are copied to the output stream beforehand.
     * 
     * @param id
     * @param dto
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void write(Text id, DTO dto ) throws IOException,
            InterruptedException {
        LOG.debug( "writing DTO ... " + id.toString() );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        List<MetsDocument> list = new LinkedList<MetsDocument>();
        try {
            if( DTO.type.equals(MetsDocument.class)) 
                list.add( (MetsDocument)dto.getObject() );
            else if( DTO.type.equals(IntellectualEntity.class)) 
                list.add( 
                    MetsUtil.convertEntity(
                        (IntellectualEntity) dto.getObject()));

            SCAPEMarshaller.getInstance().serialize(new IntellectualEntityCollection(list), baos);


        } catch (JAXBException ex) {
            throw new IOException(ex);
        }

        byte[] buf = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);

        int b;
        char[] out = new char[buf.length];
        int i = 0;
        while ((b = bais.read()) != -1) {
            out[i++] = (char) b;
        }
        LOG.debug("out = " + new String(out));
        bais.reset();

        xml = new XmlUtil(bais, tag);
        if( isFirst ) {
            xml.readDeclaration();
            xml.readRootTag();

            xml.writeDeclaration(this.out);
            xml.writeRootTag(this.out);

            isFirst = false;
        } 

        xml.readNextData();

        xml.writeNextData(this.out);
    }

}
