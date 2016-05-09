package pmel.sdig.cleaner

class LeafDataset {

    List netCDFVariables
    List badNetCDFVariables

    Status status
    String url
    String hash
    String urlPath          // Come straight from the reference
    String crawlStartTime
    String crawlEndTime
    String error = "none"

    static hasMany = [netCDFVariables: NetCDFVariable, badNetCDFVariables: BadNetCDFVariable]
    static constraints = {
        crawlStartTime nullable: true
        crawlEndTime nullable : true

    }
    static mapping = {
        netCDFVariables (cascade: 'all-delete-orphan')
        badNetCDFVariables (cascade: 'all-delete-orphan')
        url type: 'text'
        error type: 'text'
    }
    enum Status {
        CRAWL_NOT_STARTED     ("Crawl not started"),
        CRAWL_STARTED         ("Crawl started"),
        CRAWL_FAILED          ("Crawl failed"),
        CRAWL_FINISHED        ("Crawl finished")
        String name
        Status (String name) {
            this.name = name
        }
    }
    def String getRepresentativeTime() {
        // The search criteria is to find the first varible with a time axis, or null if there are none.
        NetCDFVariable v = netCDFVariables?.find{ it.timeAxis }
        if ( v ) {
            return v.timeAxis.start
        }
        // Return null if no variables with time axis found
        null
    }
}
