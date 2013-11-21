package eu.scape_project.pt.mets.hadoop.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Matthias Rella, AIT-DME
 */
public class XmlEntity {
    private String content;
    public XmlEntity(String content) {
        this.content = content;
    }

    /**
     * Retrieves the content of the first found element with the given tag
     * @param tag 
     * @return String
     */
    public String get( String tag ){
        String[] splits = tag.split("@");
        tag = splits[0];
        String attr = null;
        if( splits.length > 1 ) attr = splits[1];

        boolean onlyTag = attr == null;

        Pattern regex;
        if( onlyTag ) {
            regex = Pattern.compile("<"+tag+">(.*?)</"+tag+">");
        } else {
            regex = Pattern.compile("<"+tag+".*\\s+"+attr+"\\s*=\"(.*?)\".*>");
            System.out.println(regex);
        }

        Matcher matcher = regex.matcher(this.content);
        if( matcher.find() )
            return matcher.group(1);

        return null;
    }

    /**
     * Sets the content of the given tag
     * @param tag 
     * @return String
     */
    public void set( String tag, String newContent ){
        Pattern regex = Pattern.compile("<"+tag+">(.*?)</"+tag+">");
        Matcher matcher = regex.matcher(this.content);
        this.content = this.content.replaceFirst(
                "<"+tag+">(.*?)</"+tag+">", 
                "<"+tag+">"+newContent+"</"+tag+">"
            );
    }

    public String getContent() {
        return this.content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
