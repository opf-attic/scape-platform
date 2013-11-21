package eu.scape_project.pt.mets.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of FileOutputFormat to create Mets XML output files.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class MetsOutputFormat extends FileOutputFormat<Text, DTO> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MetsOutputFormat.class);
    public static String TAG = "MetsOutputFormat.XmlTag";

    CompressionCodec codec;
    
    @Override
    public RecordWriter<Text, DTO> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        
        Configuration conf = job.getConfiguration();
        String extension = "xml";
	String type = conf.get("DTO.type");
	try{
	    DTO.setType( Class.forName(type) );
	} catch(ClassNotFoundException ex ){
 	    LOG.error(ex.getMessage());
	}
        
	if( conf.get("mets.output.format.list").equals("true")) {
	    // create output destination
	    Path file = getDefaultWorkFile(job, "."+extension);
	
	    LOG.debug( "output file: " + file );
	
	    FileSystem fs = file.getFileSystem(conf);
	    FSDataOutputStream out = fs.create( file );

	    return new MetsRecordWriter( out, conf.get(MetsOutputFormat.TAG) );
	} else {
	    Path file = getDefaultWorkFile(job, "");
        return new SingleMetsRecordWriter( conf, file.toString(), extension, conf.get(MetsOutputFormat.TAG) );
    }

    }

}

