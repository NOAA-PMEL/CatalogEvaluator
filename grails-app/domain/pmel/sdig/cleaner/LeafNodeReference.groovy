package pmel.sdig.cleaner

class LeafNodeReference {

    String url
    String urlPath
    Status status
    String threddsid
    boolean isBestTimeseries = false


    static belongsTo = [catalog: Catalog]
    static constraints = {
        threddsid nullable: true
    }
    static mapping = {
        url type: 'text'
    }
    enum Status {
        CRAWL_NOT_STARTED     ("Crawl not started"),
        CRAWL_STARTED         ("Crawl started"),
        CRAWL_FINISHED        ("Crawl finished"),
        CRAWL_FAILED          ("Crawl failed"),
        CRAWL_SKIP            ("Do not crawl")
        String name
        Status (String name) {
            this.name = name
        }
    }

    @Override
    def String toString() {
        url
    }

}
