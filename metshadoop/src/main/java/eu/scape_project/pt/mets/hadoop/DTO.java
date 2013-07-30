/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.mets.hadoop;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.util.ScapeMarshaller;
import gov.loc.mets.MetsType;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop compatible wrapper for digital transfer objects.
 * Maybe not DTOs as such should be wrapped but MetsDocuments.
 * 
 * @author Matthias Rella [myrho]
 */
public class DTO implements Writable, Comparable, WritableComparable {

    private static final Logger LOG = LoggerFactory.getLogger(MetsRecordReader.class);
    public static Class type = String.class;

    public static void setType(Class aClass) {
        type = aClass;
    }

    Object object;

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public String getIdentifier() {
        
        if( type.equals( String.class )) {
            return findIDfromString((String)object);
        } else if( type.equals( IntellectualEntity.class) ) {
            return ((IntellectualEntity)object).getIdentifier().getValue();
        } else if( type.equals(MetsType.class)) {
            return ((MetsType)object).getID();
            //return ((MetsType)object).getID() != null
                    //? ((MetsType)object).getID() 
                    //: ((MetsType)object).getOBJID();
        }
        throw new RuntimeException( "Type " + type + " not supported" );
    }

    /**
     * Hadoop invoked serializer. 
     * 
     * @param d
     * @throws IOException 
     */
    public void write(DataOutput d) throws IOException {
        if( type.equals(String.class)) {
            d.writeUTF((String)object);
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                ScapeMarshaller.newInstance().serialize(object, baos);
            } catch (JAXBException ex) {
                throw new IOException(ex);
            }
            d.writeUTF(new String(baos.toByteArray()));
        }
    }

    /**
     * Hadoop invoked deserializer.
     * 
     * @param di
     * @throws IOException 
     */
    public void readFields(DataInput di) throws IOException {
        //StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //char c;
        String xml = di.readUTF();

        if( type.equals(String.class)) {
            object = xml;
        } else {
            ByteArrayInputStream bais = new ByteArrayInputStream( xml.getBytes() );
            try {
                if( type.equals( MetsType.class ) )
                    object = ScapeMarshaller.newInstance().deserialize(bais);
                else
                    object = ScapeMarshaller.newInstance().deserialize(type, bais);
            } catch (JAXBException ex) {
                throw new IOException(ex);
            }
        }
    }

    public int compareTo(Object o) {
        if( type.equals( MetsType.class ))
            return new Integer(getIdentifier().hashCode()).compareTo( 
                    ((MetsType)o).getID().hashCode());
        else if( type.equals( IntellectualEntity.class ))
            return new Integer(getIdentifier().hashCode()).compareTo( 
                    ((IntellectualEntity)o).getIdentifier().hashCode());
        else if( type.equals( String.class) )
            return new Integer(getIdentifier().hashCode()).compareTo(
                    findIDfromString((String)o).hashCode());
        return 0;
    }

    private String findIDfromString(String string) {
        //Pattern pattern = Pattern.compile("(ID=\"([^\"]*)\"|OBJID=\"([^\"]*)\")");
        Pattern pattern = Pattern.compile("ID=\"([^\"]*)\"");
        Matcher matcher = pattern.matcher(string);
        matcher.find();
        //return matcher.group(2) != null ? matcher.group(2) : matcher.group(3);
        return matcher.group(1);
    }
}
