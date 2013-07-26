/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package eu.scape_project.pt.mets.hadoop;

import eu.scape_project.pt.mets.hadoop.utils.XmlFSUtil;
import eu.scape_project.pt.mets.utils.XmlUtil;
import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.util.ScapeMarshaller;
import gov.loc.mets.MetsType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetsRecordReader class to read through a given xml document to output xml
 * blocks as records as specified by the start tag and end tag.
 * The records to be read must be nested within a root element in the xml document.
 * It is not allowed to have a record block as root (ie. a document with one record).
 *
 * @author Matthias Rella [myrho]
 */
public class MetsRecordReader extends RecordReader<Text, DTO> {

    private static final Logger LOG = LoggerFactory.getLogger(MetsRecordReader.class);
    /**
     * Start position of InputSplit
     */
    private long start;

    /**
     * End position of InputSplit
     */
    private long end;

    /**
     * InputStream to read data from
     */
    private FSDataInputStream fsin;

    /**
     * Value of the record
     */
    private DTO value;

    /**
     * Key of the record
     */
    private Text key;

    /**
     * XmlUtil
     */
    private XmlUtil xml;

    /**
     * Tag name of record element
     */
    private final String tag;

    public MetsRecordReader( String tag ) {
        this.tag = tag;
    }

    /**
     *
     * @param genericSplit
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration jobConf = context.getConfiguration();

        FileSplit split = (FileSplit) genericSplit;

        // open the file and seek to the start of the split
        start = split.getStart();
        LOG.debug("start = " + start);
        end = start + split.getLength();
        LOG.debug("end = " + end);

        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(jobConf);
        fsin = fs.open(split.getPath());

        xml = new XmlFSUtil(fsin, end, tag);

        xml.readDeclaration();
        xml.readRootTag();

        fsin.seek(start);
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
        float retVal = (fsin.getPos() - start) / (float) (end - start);
        return retVal < 1 ? retVal : 1F;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        LOG.debug("reading nextKeyvalue");
        LOG.debug("fsin.getPos = " + fsin.getPos() + ", end = " + end);

        // sets rawData, or not if no more available
        if( xml.readNextData() == null ) return false; 

        value = new DTO();
        value.setObject(createDTO()); 

        LOG.debug("value.getIdentifier() = " + value.getIdentifier());
        key = new Text(value.getIdentifier());
        return true;
    }

    public byte[] getRawData() {
        return xml.getRawData();
    }

    /**
     * Stick together raw XML portions to create the DTO XML record.
     * Appends the attributes of the root element to the tag of the DTO record
     * (it's about the namespace definitions).
     *
     * @return
     * @throws IOException
     */
    private Object createDTO() throws IOException {

        byte[] buf = xml.getRecord();

        ByteArrayInputStream bais = new ByteArrayInputStream(buf);

        int b;
        char[] out = new char[buf.length];
        int i = 0;
        while ((b = bais.read()) != -1) {
            out[i++] = (char) b;
        }
        LOG.debug("out = " + new String(out));
        bais.reset();
        try {
            if( DTO.type.equals(MetsType.class) )
                return
                    ScapeMarshaller.newInstance().deserialize(bais);
            else
                return 
                    ScapeMarshaller.newInstance().deserialize(DTO.type, bais);

        } catch (JAXBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public DTO getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
}
