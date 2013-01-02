/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.pt.metshadoop;

import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.IntellectualEntityCollection;
import eu.scapeproject.model.mets.SCAPEMarshaller;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Hadoop compatible wrapper for digital transfer objects.
 * Maybe not DTOs as such should be wrapped but MetsDocuments.
 * 
 * @author Matthias Rella [myrho]
 */
class DTO implements Writable, Comparable, WritableComparable {

    public static Class type = MetsDocument.class;

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
        } else if( type.equals(MetsDocument.class)) {
            return ((MetsDocument)object).getId();
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
            if( object instanceof MetsDocument )
                SCAPEMarshaller.getInstance()
                        .getJaxbMarshaller().marshal(object, baos);
            else
                SCAPEMarshaller.getInstance().serialize(object, baos);
        } catch (JAXBException ex) {
            throw new IOException(ex);
        }
        d.write(baos.toByteArray());
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
        byte b;
        try {
            while (true) {
                //c = di.readChar();
                b = di.readByte();
                //sb.append(c);
                baos.write(b);
            }
        }catch( Exception e ) {};
        ByteArrayInputStream bais = new ByteArrayInputStream( baos.toByteArray() );
        try {
            if( type.equals( MetsDocument.class ) )
                object = SCAPEMarshaller.getInstance()
                        .getJaxbUnmarshaller().unmarshal(bais);
            else
                object = SCAPEMarshaller.getInstance().deserialize(type, bais);
        } catch (JAXBException ex) {
            throw new IOException(ex);
        }
    }

    public int compareTo(Object o) {
        if( type.equals( MetsDocument.class ))
            return new Integer(getIdentifier().hashCode()).compareTo( 
                    ((MetsDocument)o).getId().hashCode());
        return 0;
    }
}
