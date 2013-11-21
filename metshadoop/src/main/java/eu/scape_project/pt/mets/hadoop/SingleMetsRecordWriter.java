package eu.scape_project.pt.mets.hadoop;

import eu.scape_project.pt.mets.utils.XmlUtil;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.IntellectualEntityCollection;
import eu.scapeproject.util.DefaultConverter;
import eu.scapeproject.util.ScapeMarshaller;
import gov.loc.mets.MetsType;
//import eu.scapeproject.model.util.MetsUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class SingleMetsRecordWriter extends RecordWriter<Text, DTO> {
    
    private static final Logger LOG = LoggerFactory.getLogger(SingleMetsRecordWriter.class);

    String filePrefix;

    /**
     * Tag name of record
     */
    private final String tag;
    private boolean isFirst = true;
	private Configuration conf;
	private String extension;

    public SingleMetsRecordWriter(Configuration conf, String filePrefix, String extension, String tag ) {
        this.filePrefix = filePrefix;
        this.extension = extension;
        this.tag = tag;
	    this.conf = conf;
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

        Path file = new Path(this.filePrefix + id.toString() + "." + this.extension);
        FileSystem fs = file.getFileSystem(this.conf);
        FSDataOutputStream fos = fs.create( file );
        DefaultConverter conv = new DefaultConverter();
        IntellectualEntity ie = null;
        try {
            if( DTO.type.equals(MetsType.class)) {
                ie = conv.convertMets((MetsType)dto.getObject());
		        LOG.debug("isMetsType");
            } else if( DTO.type.equals(IntellectualEntity.class))  {
                ie = (IntellectualEntity)dto.getObject();
                LOG.debug("isIE");
            }

            ScapeMarshaller.newInstance().serialize(ie, fos);

        } catch (JAXBException ex) {
            throw new IOException(ex);
        } finally {
            fos.close();
        }

    }

}
