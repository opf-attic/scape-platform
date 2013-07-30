package eu.scape_project.pt.mets.hadoop.dummy;

import eu.scape_project.pt.mets.hadoop.DTO;
import eu.scape_project.pt.mets.hadoop.MetsInputFormat;
import eu.scape_project.pt.mets.hadoop.MetsOutputFormat;
import eu.scapeproject.ConnectorAPIMock;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.IntellectualEntityCollection;
import eu.scapeproject.util.ScapeMarshaller;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.purl.dc.elements._1.ElementContainer;
import org.purl.dc.elements._1.SimpleLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates usage of the Connector API within a hadoop job.
 * Fetches XML data from the Repository and processes it using
 * MetsInputFormat. 
 * 
 * TODO: As a Reduce creates/updates XML using MetsOutputFormat.
 * TODO: Finally uploads new XML to Repository through Connector API.
 *
 */
public class DummyJob extends Configured implements Tool
{
    private static final ConnectorAPIMock MOCK = new ConnectorAPIMock(8387);
    private static final ConnectorAPIUtil UTIL = new ConnectorAPIUtil("http://localhost:8387");
    private static final HttpClient CLIENT = new DefaultHttpClient();
    private static final Logger LOG = LoggerFactory.getLogger(DummyJob.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main( String[] args ) 
    {
        try {
            Thread t = new Thread(MOCK);
            t.start();
            while (!MOCK.isRunning()) {
                Thread.sleep(10);
            }

            // ingest entity 1
            List<String> ids = new ArrayList<String>();
            IntellectualEntity entity1 = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .descriptive(createDCElementContainer("a test entity"))
                .build();
            ids.add(entity1.getIdentifier().getValue());
            HttpPost post = UTIL.createPostEntity(entity1);
            HttpResponse resp = CLIENT.execute(post);
            post.releaseConnection();
            LOG.debug("ingested entity 1: " + resp.getStatusLine().getStatusCode());

            // ingest entity 2
            IntellectualEntity entity2 = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .descriptive(createDCElementContainer("another test entity"))
                .build();
            ids.add(entity2.getIdentifier().getValue());
            post = UTIL.createPostEntity(entity2);
            resp = CLIENT.execute(post);
            post.releaseConnection();
            LOG.debug("ingested entity 2: " + resp.getStatusLine().getStatusCode());

            // get XML from Connector and write it to HDFS here

            List<IntellectualEntity> all = new ArrayList<IntellectualEntity>();
            all.add(entity1);
            all.add(entity2);

            Path entitiesPath = new Path( "entities.xml");
            FileSystem fs = entitiesPath.getFileSystem(new Configuration());
            FSDataOutputStream fsout = fs.create(entitiesPath);
            fs.delete(new Path("output"), true);

            ScapeMarshaller.newInstance().serialize(
                    new IntellectualEntityCollection(all), fsout);

            post.releaseConnection();

            String[] strings = new String[1];
            strings[0] = entitiesPath.toString();

	        fsout.close();

            int res = ToolRunner.run(new Configuration(), new DummyJob(), strings);

            MOCK.stop();
            MOCK.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Path file_on_hdfs = new Path( strings[0] );

        conf.set(MetsInputFormat.TAG, "mets:mets");
        conf.set(MetsOutputFormat.TAG, "mets:mets");

        Job job = new Job(conf);

        // configure job

        job.setJobName("dummy-job-using-connector-api");
        job.setJarByClass(DummyJob.class);

        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        job.setOutputValueClass(DTO.class);

        job.setMapperClass(DummyJobMapper.class);

        //job.setReducerClass(DummyJobReducer.class);

        job.setInputFormatClass(MetsInputFormat.class);
        job.setOutputFormatClass(MetsOutputFormat.class);

        LOG.debug("file_on_hdfs = " + file_on_hdfs.toString() );
        MetsInputFormat.setInputPaths(job, file_on_hdfs);
        MetsOutputFormat.setOutputPath(job, new Path("output"));

	//FileInputFormat.setMaxInputSplitSize(job, 1024);

        LOG.debug("maxsplitsize = " + FileInputFormat.getMaxSplitSize(job));

        job.waitForCompletion(true);
        return 0;
    }

    static public ElementContainer createDCElementContainer(String strTitle){
        ElementContainer c = new ElementContainer();
        
        SimpleLiteral title = new SimpleLiteral();
        title.getContent().add(strTitle);
        c.getAny().add(new JAXBElement<SimpleLiteral>(new QName("http://purl.org/dc/elements/1.1/", "title"), SimpleLiteral.class, title));
        
        SimpleLiteral date = new SimpleLiteral();
        date.getContent().add(dateFormat.format(new Date()));
        c.getAny().add(new JAXBElement<SimpleLiteral>(new QName("http://purl.org/dc/elements/1.1/", "created"), SimpleLiteral.class, date));
        
        SimpleLiteral lang = new SimpleLiteral();
        lang.getContent().add("en");
        c.getAny().add(new JAXBElement<SimpleLiteral>(new QName("http://purl.org/dc/elements/1.1/", "created"), SimpleLiteral.class, lang));
        
        return c;
    }
}
