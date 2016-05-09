package pmel.sdig.cleaner.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class Util {
    public static String getUrl(String originalUrl, String stcatalogrefUri, String thredds) throws URISyntaxException{
        originalUrl = originalUrl.substring(0, originalUrl.lastIndexOf("/")+1);
        URI uri1 = new URI(originalUrl); 
        String base = uri1.getHost();
        if(uri1.getPort() > 0){
            base += ":" + uri1.getPort();
        }
        base += "/";
        URI catalogrefUri = new URI(stcatalogrefUri);
        if(!catalogrefUri.isAbsolute()){
            if(stcatalogrefUri.startsWith("/" + thredds + "/") && base.endsWith(thredds + "/")){
                stcatalogrefUri = stcatalogrefUri.substring(1 + thredds.length(), stcatalogrefUri.length());
            }
            if(stcatalogrefUri.startsWith("/")){
                URI remoteURI = new URI(base);
                catalogrefUri = new URI(remoteURI.toString() + stcatalogrefUri.replaceFirst("/", ""));
            }
            else{
                //URI url = new URI(rawCatalogref.getPrivateUrl().getValue());
                if(stcatalogrefUri.startsWith("./"))
                    stcatalogrefUri = stcatalogrefUri.substring(2, stcatalogrefUri.length());
                catalogrefUri = new URI(originalUrl + stcatalogrefUri);
            }
        }
        String url = catalogrefUri.toString();
        if(!url.startsWith("http://"))
            url = "http://" + url;

        return url;
    }
    /**
     * http://en.wikibooks.org/wiki/Algorithm_implementation/Strings/Longest_common_substring#Java
     * @param S1
     * @param S2
     * @return
     */
    public static String longestCommonSubstring(String S1, String S2)
    {
        int Start = 0;
        int Max = 0;
        for (int i = 0; i < S1.length(); i++)
        {
            for (int j = 0; j < S2.length(); j++)
            {
                int x = 0;
                while (S1.charAt(i + x) == S2.charAt(j + x))
                {
                    x++;
                    if (((i + x) >= S1.length()) || ((j + x) >= S2.length())) break;
                }
                if (x > Max)
                {
                    Max = x;
                    Start = i;
                }
             }
        }
        return S1.substring(Start, (Start + Max));
    }
    public static List<String> uniqueParts(int iterations, String s1, String s2) {
        for ( int i = 0; i < iterations; i++ ) {
            String a = Util.longestCommonSubstring(s1, s2);
            s1 = s1.replace(a, "");
            s2 = s2.replace(a, "");
        }
        List<String> parts = new ArrayList<String>();
        parts.add(s1);
        parts.add(s2);
        return parts;
    }
}
