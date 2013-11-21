/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.mets.hadoop;

import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.pt.mets.hadoop.utils.XmlEntity;
import eu.scape_project.util.ScapeMarshaller;
import gov.loc.mets.MetsType;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.JAXBException;
import org.apache.commons.io.IOUtils;
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
    public static Class type = XmlEntity.class;

    public static void setType(Class aClass) {
        type = aClass;
    }

    public static Object deserialize(InputStream in ) throws IOException {
        try {
            if(type.equals(String.class)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                IOUtils.copy(in, baos);
                return baos.toString();
            } else if(type.equals(XmlEntity.class)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                IOUtils.copy(in, baos);
                return new XmlEntity(baos.toString());
            } else if( type.equals( MetsType.class ) )
                return ScapeMarshaller.newInstance().deserialize(in);
            else
                return ScapeMarshaller.newInstance().deserialize(type, in);
        } catch (JAXBException ex) {
            throw new IOException(ex);
        }
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
        } else if( type.equals( XmlEntity.class )) {
            return ((XmlEntity)object).get("mets:mets@ID");
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
        if( type.equals(XmlEntity.class)) {
            d.writeUTF(((XmlEntity)object).getContent());
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
        } else if( type.equals(XmlEntity.class)) {
            ((XmlEntity)object).setContent(xml);
        } else {
            ByteArrayInputStream bais = new ByteArrayInputStream( xml.getBytes() );
            object = deserialize(bais);
        }
    }

    public int compareTo(Object o) {
        if( type.equals( MetsType.class ))
            return new Integer(getIdentifier().hashCode()).compareTo( 
                    ((MetsType)o).getID().hashCode());
        else if( type.equals( IntellectualEntity.class ))
            return new Integer(getIdentifier().hashCode()).compareTo( 
                    ((IntellectualEntity)o).getIdentifier().hashCode());
        else if( type.equals( XmlEntity.class) )
            return new Integer(getIdentifier().hashCode()).compareTo(
                    ((XmlEntity)o).get("mets:mets@ID").hashCode());
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
