package pmel.sdig.cleaner;

import org.jdom2.Element;
import org.jdom2.filter.AbstractFilter;


public class UrlPathFilter extends AbstractFilter {
    private String urlPath;
    public UrlPathFilter(String urlPath) {
        this.urlPath = urlPath;
    }
    @Override
    public Object filter(Object obj) {
        if ( obj instanceof Element ) {
            Element e = (Element) obj;
            if ( e.getAttributeValue("urlPath") != null &&  e.getAttributeValue("urlPath").equals(urlPath) ) return obj;
        }
        return null;
    }
    
}
