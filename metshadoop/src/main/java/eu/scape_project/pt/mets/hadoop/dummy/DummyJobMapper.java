package eu.scape_project.pt.mets.hadoop.dummy;

import eu.scape_project.pt.mets.hadoop.DTO;
import eu.scapeproject.model.IntellectualEntity;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives a DTO and does something with it. Passes it on.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class DummyJobMapper extends Mapper<Text, DTO, Text, DTO> {
	private static final Logger LOG = LoggerFactory.getLogger(DummyJobMapper.class);

    @Override
    public void setup(Context context) {
	Configuration conf = context.getConfiguration();
	String type = conf.get("DTO.type");
	try{
	    DTO.setType( Class.forName(type) );
	} catch(ClassNotFoundException ex ){
 	    LOG.error(ex.getMessage());
	}
    }

    @Override
    public void map(Text key, DTO value, Context context)
            throws IOException, InterruptedException {

        System.out.println( "DTO is landed: id = " + value.getIdentifier() );

        context.write(key, value);

    }
    
}
