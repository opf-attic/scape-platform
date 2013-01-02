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
package eu.scape_project.pt.metshadoop;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.metadata.textmd.TextMDMetadata;
import eu.scapeproject.model.mets.SCAPEMarshaller;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.logging.Level;
import javax.xml.bind.JAXBException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
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
 *
 * @author Matthias Rella [myrho]
 */
public class MetsRecordReader extends RecordReader<Text, DTO> {

    private static final Logger LOG = LoggerFactory.getLogger(MetsRecordReader.class);
    public static String ENCODING = "utf-8";
    public static String TAG = "XmlStartTag";

    /**
     * XML Element tag to start reading record at
     */
    private byte[] startTag;

    /**
     * XML Element tag to end record at
     */
    private byte[] endTag;

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
     * Buffer to put record data to
     */
    private DataOutputBuffer buffer = new DataOutputBuffer();

    /**
     * Value of the record
     */
    private DTO value;

    /**
     * Key of the record
     */
    private Text key;

    /**
     * Raw Data for a record
     */
    private byte[] rawData;

    /**
     * portion containing XML declaration
     */
    private byte[] decl;

    /**
     * portion containing the root element tag
     */
    private byte[] root;

    /**
     * portion containing the attributes of the root element
     */
    private byte[] rootAttr;

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
        // construct start and end tag of record element
        startTag = ("<" + jobConf.get(TAG) + " ").getBytes(ENCODING);
        endTag = ("</" + jobConf.get(TAG) + ">").getBytes(ENCODING);

        FileSplit split = (FileSplit) genericSplit;

        // open the file and seek to the start of the split
        start = split.getStart();
        LOG.debug("start = " + start);
        end = start + split.getLength();
        LOG.debug("end = " + end);

        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(jobConf);
        fsin = fs.open(split.getPath());

        setRootData();

        fsin.seek(start);
    }

    /**
     * Sets the root element name and its attributes.
     *
     * @throws IOException
     */
    private void setRootData() throws IOException {
        buffer = new DataOutputBuffer();
        fsin.seek(0L);
        // get xml decl definition
        if (readUntilMatch("<?xml".getBytes(ENCODING), false)) {
            buffer.write("<?xml".getBytes(ENCODING));
            readUntilMatch("?>".getBytes(ENCODING), true);
            buffer.write("?>".getBytes(ENCODING));
        }
        decl = Arrays.copyOf(buffer.getData(), buffer.size());

        LOG.debug("decl = " + new String(decl, Charset.forName(ENCODING)) + ", len = " + decl.length);

        buffer = new DataOutputBuffer();
        // get root element tag
        if (readUntilMatch("<".getBytes(ENCODING), false)) {
            readUntilMatch(" ".getBytes(ENCODING), true);
        }

        root = Arrays.copyOf(buffer.getData(), buffer.size());

        LOG.debug("root = " + new String(root) + ", len = " + root.length);

        buffer = new DataOutputBuffer();
        // read attributes of root
        readUntilMatch(">".getBytes(ENCODING), true);
        rootAttr = Arrays.copyOf(buffer.getData(), buffer.size());
        LOG.debug("rootAttr = " + new String(rootAttr) + ", len = " + rootAttr.length);

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

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        int[] matched = new int[match.length];
        while (true) {
            int b = fsin.read();

            //LOG.debug("b = " + (char)(b));

            // end of file:
            if (b == -1) {
                LOG.debug("b = -1");
                return false;
            }

            // check if we're matching:
            if (b == match[i]) {
                matched[i] = b;
                i++;
                if (i >= match.length) {
                    return true;
                }
            } else {
                if (withinBlock) {
                    // not matched, write partly matched data to buffer
                    for (int j = 0; j < i; j++) {
                        buffer.write(matched[j]);
                    }
                    // write current not-matching byte to buffer:
                    buffer.write(b);
                }
                i = 0;
            }


            // see if we've passed the stop point:
            if (!withinBlock && i == 0 && fsin.getPos() >= end) {
                LOG.debug("passing the end point");
                return false;
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        LOG.debug("reading nextKeyvalue");
        LOG.debug("fsin.getPos = " + fsin.getPos() + ", end = " + end);

        // sets rawData, or not if no more available
        if( nextData() == null ) return false; 

        value = new DTO();
        readDTO(); // sets value
        key = new Text(value.getIdentifier());
        return true;
    }

    /**
     * Reads next data record ie the attributes of the xml element (without
     * element name), its contents and the end tag.
     *
     * @return
     * @throws IOException
     */
    private byte[] nextData() throws IOException {
        buffer = new DataOutputBuffer();
        rawData = null;
        if (fsin.getPos() < end) {
            LOG.debug("startTag = " + new String(startTag));
            if (readUntilMatch(startTag, false)) {
                //buffer.write(startTag);
                if (readUntilMatch(endTag, true)) {
                    LOG.debug("buffer contains then: " + new String(buffer.getData()));
                    buffer.write(endTag);
                    // build valid xml with one record
                    rawData = Arrays.copyOf(buffer.getData(), buffer.size());
                    return rawData;
                }
            }
        }
        return rawData;
    }

    public byte[] getRawData() {
        return rawData;
    }

    /**
     * Stick together raw XML portions to create the DTO XML record.
     * Appends the attributes of the root element to the tag of the DTO record
     * (it's about the namespace definitions).
     *
     * @return
     * @throws IOException
     */
    private DTO readDTO() throws IOException {

        byte[] buf = concatAll(decl, startTag, rootAttr, " ".getBytes(ENCODING), rawData);

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
            //value.setObject(SCAPEMarshaller.getInstance().deserialize(DTO.type, bais));
            value.setObject(
                SCAPEMarshaller.getInstance()
                    .getJaxbUnmarshaller().unmarshal(bais));

        } catch (JAXBException e) {
            throw new IOException(e);
        }
        return value;
    }

    public static byte[] concatAll(byte[] first, byte[]... rest) {
        int totalLength = first.length;
        for (byte[] array : rest) {
            totalLength += array.length;
        }
        byte[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (byte[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
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
