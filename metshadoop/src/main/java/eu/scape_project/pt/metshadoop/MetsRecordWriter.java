package eu.scape_project.pt.metshadoop;

import eu.scapeproject.model.mets.SCAPEMarshaller;
import java.io.IOException;

import javax.xml.bind.JAXBException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes AVPackets to an underlying AVOutputStream.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class MetsRecordWriter extends RecordWriter<Text, DTO> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MetsRecordWriter.class);

    FSDataOutputStream out;

    public MetsRecordWriter(FSDataOutputStream fileOut) {
        out = fileOut;
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
            InterruptedException {
        out.close();

    }

    @Override
    public void write(Text id, DTO dto ) throws IOException,
            InterruptedException {
        LOG.debug( "writing DTO ... " + id.toString() );
        try {
            SCAPEMarshaller.getInstance().serialize(dto.getObject(), out);
        } catch (JAXBException ex) {
            throw new IOException(ex);
        }
    }

}

