/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.mets.hadoop;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.util.ScapeMarshaller;
import gov.loc.mets.MetsType;
import java.io.*;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Hadoop compatible wrapper for digital transfer objects.
 * Maybe not DTOs as such should be wrapped but MetsDocuments.
 * 
 * @author Matthias Rella [myrho]
 */
public class DTO implements Writable, Comparable, WritableComparable {

    public static Class type = MetsType.class;

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
        
        if( type.equals( IntellectualEntity.class) ) {
            return ((IntellectualEntity)object).getIdentifier().getValue();
        } else if( type.equals(MetsType.class)) {
            return ((MetsType)object).getID() != null
                    ? ((MetsType)object).getID() 
                    : ((MetsType)object).getOBJID();
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ScapeMarshaller.newInstance().serialize(object, baos);
        } catch (JAXBException ex) {
            throw new IOException(ex);
        }
        d.writeUTF(new String(baos.toByteArray()));
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

    public int compareTo(Object o) {
        if( type.equals( MetsType.class ))
            return new Integer(getIdentifier().hashCode()).compareTo( 
                    ((MetsType)o).getID().hashCode());
        return 0;
    }
}
