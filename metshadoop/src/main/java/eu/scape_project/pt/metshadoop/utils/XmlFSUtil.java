/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.scape_project.pt.metshadoop.utils;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Matthias Rella, DME-AIT
 */
public class XmlFSUtil extends XmlUtil {

    private static final Logger LOG = LoggerFactory.getLogger(XmlFSUtil.class);
    /**
     * Where to stop reading the stream
     */
    private final long end;

    public XmlFSUtil(FSDataInputStream in, long end, String tag ) throws IOException {
        super(in, tag);
        this.end = end;
    }

    /**
     * Reads until matches given match string in input stream. 
     * The last record it reads from the input stream is that which contains
     * the end point.
     * 
     * @param match
     * @param withinBlock
     * @return
     * @throws IOException 
     */
    @Override
    protected boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        int[] matched = new int[match.length];
        while (true) {
            int b = in.read();

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
            if (!withinBlock && i == 0 && ((FSDataInputStream)in).getPos() >= end) {
                LOG.debug("passing the end point");
                return false;
            }
        }
    }

    /**
     * Reads the XML declaration. Seeks to the beginning of the stream beforehand.
     * 
     * @throws IOException 
     */
    @Override
    public void readDeclaration() throws IOException {
        ((FSDataInputStream)in).seek(0L);
        super.readDeclaration();
    }

    /**
     * Reads next data record ie the attributes of the xml element (without
     * element name), its contents and the end tag. If end point is passed
     * returns null.
     *
     * @return
     * @throws IOException
     */
    @Override
    public byte[] readNextData() throws IOException {
        if( ((FSDataInputStream)in).getPos() >= end ) return null;

        return super.readNextData();
    }


    

}
