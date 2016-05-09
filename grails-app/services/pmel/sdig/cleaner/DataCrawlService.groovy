package pmel.sdig.cleaner

import grails.transaction.Transactional

class DataCrawlService {
    static transactional = false
    IngestService ingestService

    AsyncCleanService asyncCleanService
    CleanService cleanService
    CountService countService

    def chainedDataCrawl(String parent, String caturl) {
        dataCrawl(parent, caturl)
    }
    def void dataCrawl(String parent, String caturl) {

        try {

            //TODO Figure out what should be nullable in the domain classes
            def catalog = Catalog.findByParentAndUrl(parent,caturl)

            if ( catalog.status != Catalog.Status.DOWNLOAD_FAILED ) {

                catalog.setStatus(Catalog.Status.DATA_CRAWL_STARTED)
                Catalog.lock(5)
                catalog.save(failOnError: true, flush: true)

                log.debug("Found catalog: " + catalog.id)

                rundatacrawl(catalog)

                catalog.setStatus(Catalog.Status.DATA_CRAWL_FINISHED)
                catalog.lock(6)
                catalog.save(failOnError: true, flush: true)

                log.debug("makerubric complete...")

            }

        } catch (Exception e) {
            log.debug("We tried, but we failed. "+e.message)
        }

    }

    def void rundatacrawl(Catalog catalog) {
        int dataSourceCount = 1;
        if ( catalog.leafNodeReferences ) {
            catalog.leafNodeReferences.each { LeafNodeReference lnr ->
                log.debug("Scanning data set $dataSourceCount of ${catalog.leafNodeReferences.size()}")
                ingestService.ingestDataSource(lnr);
                dataSourceCount++;
            }
        }

        if (catalog.subCatalogs) {

            catalog.subCatalogs.each { Catalog child ->

                child.setStatus(Catalog.Status.DATA_CRAWL_STARTED)
                Catalog.lock(7)
                child.save(failOnError: true, flush: true)
                rundatacrawl(child)
                child.setStatus(Catalog.Status.DATA_CRAWL_FINISHED)
                Catalog.lock(8)
                child.save(failOnError: true, flush: true)

            }
        }
        catalog.setStatus(Catalog.Status.DATA_CRAWL_FINISHED)
        catalog.lock(9)
        catalog.save(failOnError: true, flush: true)
    }
}
