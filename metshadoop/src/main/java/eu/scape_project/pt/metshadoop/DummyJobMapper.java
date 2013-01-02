package eu.scape_project.pt.metshadoop;

import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.IntellectualEntity;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Matthias Rella, DME-AIT
 */
public class DummyJobMapper extends Mapper<Text, DTO, Text, DTO> {
	private static final Logger LOG = LoggerFactory.getLogger(DummyJobMapper.class);

    @Override
    public void setup(Context context) {
        DTO.setType( MetsDocument.class );
    }

    @Override
    public void map(Text key, DTO value, Context context)
            throws IOException, InterruptedException {
        LOG.debug( "DTO is landed: id = " + value.getIdentifier() );

    }
    
}
