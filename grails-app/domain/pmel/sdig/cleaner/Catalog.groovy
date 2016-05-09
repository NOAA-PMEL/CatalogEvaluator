package pmel.sdig.cleaner

class Catalog {
    String url
    String parent
    boolean root = false
    static hasMany = [subCatalogs: Catalog, leafNodeReferences: LeafNodeReference]
    static hasOne = [parentCatalog: Catalog, rubric: Rubric]
    Status status
    int leafCount
    static mapping = {
        status(enumType: "string")
        subCatalogs (cascade: 'all-delete-orphan')
        leafNodeReferences (cascade: 'all-delete-orphan')
        rubric (cascade: 'all-delete-orphan')
        url type: 'text'
    }
    enum Status {
        CRAWL_NOT_STARTED      ("Crawl not started"),
        TREE_CRAWL_STARTED     ("Tree Crawl started"),
        TREE_CRAWL_FINISHED    ("Tree Crawl finished"),
        DATA_CRAWL_STARTED     ("Data Crawl started"),
        DATA_CRAWL_FINISHED    ("Data Crawl finished"),
        CLEAN_STARTED          ("Clean started"),
        CLEAN_FINISHED         ("Clean finished"),
        DO_NOT_CRAWL           ("Do not crawl"),
        DOWNLOAD_FAILED        ("Download failed"),
        CRAWL_FINISHED         ("Crawl finished")
        String name
        Status (String name) {
            this.name = name
        }
    }
    static constraints = {
        parent nullable:true
        subCatalogs nullable: true
        leafNodeReferences nullable: true
        rubric nullable: true
        parentCatalog nullable: true
    }
}
