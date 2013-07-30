/*
 * To change this template, choose Tools | Templates
 * and open the template fsin the editor.
 */
package eu.scape_project.pt.mets.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import org.apache.hadoop.io.DataOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Matthias Rella, DME-AIT
 */
public class XmlUtil {

    private static final Logger LOG = LoggerFactory.getLogger(XmlUtil.class);
    public static String ENCODING = "utf-8";

    /**
     * XML Element tag to start reading record at
     */
    private byte[] startTag;

    /**
     * XML Element tag to end record at
     */
    private byte[] endTag;

    /**
     * Buffer to put record data to
     */
    protected ByteArrayOutputStream buffer = new ByteArrayOutputStream();

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
     * Inputstream to read XML from
     */
    protected final InputStream in;

    public XmlUtil( InputStream in, String tag) throws IOException {
        // construct start and end tag of record element
        startTag = ("<" + tag + " ").getBytes(ENCODING);
        endTag = ("</" + tag + ">").getBytes(ENCODING);

        this.in = in;
    }

    /**
     * Reads until matches given match string in input stream. 
     * 
     * @param match
     * @param withinBlock
     * @return
     * @throws IOException 
     */
    protected boolean readUntilMatch(byte[] match, boolean withinBlock) 
            throws IOException 
    {
        int i = 0;
        int[] matched = new int[match.length];
        while (true) {
            int b = in.read();

            //LOG.debug("checked = " + (char)(checked));

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

        }
    }

    /** 
     * Checks whether inputstream has given byte array at the current position.
     * Returns an array of bytes that have been read and checked.
     * @param bytes
     * @return 
     */
    private byte[] hasAtCurrentPos(byte[] match) throws IOException {
        int i = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();

            // end of file:
            if (b == -1) break;

            baos.write(b);

            // check if we're matching:
            if (b == match[i]) {
                i++;
                if (i >= match.length) break;
            } else break;
        }
        return baos.toByteArray();
    }

    /**
     * Reads the XML declaration and or the root tag. 
     * 
     * @throws IOException 
     */
    public void readDeclarationOrRootTag() throws IOException {
        buffer = new ByteArrayOutputStream();
        // get xml decl definition
        if( !Arrays.equals(hasAtCurrentPos("<".getBytes(ENCODING)), "<".getBytes(ENCODING)))
            return;

        byte[] xmldecl = "?xml".getBytes(ENCODING);
        byte[] checked = hasAtCurrentPos(xmldecl);
        if ( Arrays.equals(checked, xmldecl)) {
            buffer.write("<".getBytes(ENCODING));
            buffer.write(xmldecl);
            readUntilMatch("?>".getBytes(ENCODING), true);
            buffer.write("?>".getBytes(ENCODING));
            decl = buffer.toByteArray();
            readRootTag();
        } else {
            decl = new byte[0];
            // finish root element tag 
            readUntilMatch(" ".getBytes(ENCODING), true);

            root = concatAll( checked, buffer.toByteArray());

            LOG.debug("root = " + new String(root, ENCODING) + ", len = " + root.length);

            buffer = new ByteArrayOutputStream();
            // read attributes of root
            readUntilMatch(">".getBytes(ENCODING), true);
            // TODO only read xmlns attributes!
            rootAttr = buffer.toByteArray();
            LOG.debug("rootAttr = " + new String(rootAttr) + ", len = " + rootAttr.length);
        }

        LOG.debug("decl = " + new String(decl, Charset.forName(ENCODING)) + ", len = " + decl.length);
    }
    /**
     * Reads the XML declaration. Assumes that the stream pointer is 
     * at the beginning.
     * 
     * @throws IOException 
     */
    public void readDeclaration() throws IOException {
        buffer = new ByteArrayOutputStream();
        // get xml decl definition
        if (readUntilMatch("<?xml".getBytes(ENCODING), false)) {
            buffer.write("<?xml".getBytes(ENCODING));
            readUntilMatch("?>".getBytes(ENCODING), true);
            buffer.write("?>".getBytes(ENCODING));
        }
        decl = buffer.toByteArray();

        LOG.debug("decl = " + new String(decl, Charset.forName(ENCODING)) + ", len = " + decl.length);
    }

    /**
     * Reads the root tag name and its attributes.
     * 
     * @throws IOException 
     */
    public void readRootTag() throws IOException {
        buffer = new ByteArrayOutputStream();
        // get root element tag
        if (readUntilMatch("<".getBytes(ENCODING), false)) {
            readUntilMatch(" ".getBytes(ENCODING), true);
        }

        root = buffer.toByteArray();

        LOG.debug("root = " + buffer.toString() + ", len = " + root.length);

        buffer = new ByteArrayOutputStream();
        // read attributes of root
        readUntilMatch(">".getBytes(ENCODING), true);
        // TODO only read xmlns attributes!
        rootAttr = buffer.toByteArray();
        LOG.debug("rootAttr = " + new String(rootAttr) + ", len = " + rootAttr.length);

    }

    /**
     * Reads next data record ie the attributes of the xml element (without
     * element name), its contents and the end tag.
     *
     * @return
     * @throws IOException
     */
    public byte[] readNextData() throws IOException {
        buffer = new ByteArrayOutputStream();
        rawData = null;
        LOG.debug("startTag = " + new String(startTag));
        if (readUntilMatch(startTag, false)) {
            //buffer.write(startTag);
            if (readUntilMatch(endTag, true)) {
                LOG.debug("buffer contains then: " + new String(buffer.toString().getBytes(ENCODING)));
                buffer.write(endTag);
                // build valid xml with one record
                rawData = buffer.toByteArray();
                return rawData;
            }
        }
        return rawData;
    }

    public byte[] getRawData() {
        return rawData;
    }

    /**
     * Sticks together the XML portions to get valid XML for the single record.
     * 
     * @return
     * @throws IOException 
     */
    public byte[] getRecord() throws IOException {
        return concatAll(decl, startTag, rootAttr, " ".getBytes(ENCODING), rawData);
    }

    public static byte[] concatAll(byte[] first, byte[]... rest) {
        int i, lenOfReadBytes; 
        int totalLength = lenOfReadBytes = i = first != null ? first.length : 0;
        for (byte[] array : rest) {
            if( array != null) totalLength += array.length;
        }
        byte[] result = new byte[totalLength];

        if( first != null ) {
            System.arraycopy(first, 0, result, 0, first.length );
        }

        int b = 0;
        for( ; i < totalLength; i++ ) {
            if( rest == null ) b++;
            if( i >= rest[b].length + lenOfReadBytes ) {
                lenOfReadBytes += rest[b++].length;
            }
            result[i] = rest[b][i-lenOfReadBytes];
        }
        return result;
    }

    public void writeDeclaration(OutputStream out) throws IOException {
        if( decl != null ) out.write(decl);
    }

    public void writeRootTag(OutputStream out) throws IOException {
        out.write(concatAll("<".getBytes(ENCODING), root, " ".getBytes(ENCODING), rootAttr, ">".getBytes(ENCODING)));
    }

    public void writeNextData(OutputStream out) throws IOException {
        out.write(concatAll(startTag, " ".getBytes(ENCODING), rawData));
    }

    public void writeClosingRootTag(OutputStream out) throws IOException {
        out.write(("</" + new String(root) + ">").getBytes(ENCODING));
    }

    public void reset() throws IOException {
        in.reset();
    }



}
