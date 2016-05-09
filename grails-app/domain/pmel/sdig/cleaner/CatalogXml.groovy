package pmel.sdig.cleaner

class CatalogXml {

    String url
    String xml

    static mapping = {
        // TODO should be long text in mysql production under mysql
        xml type: 'text'
        url type: 'text'
    }


}
