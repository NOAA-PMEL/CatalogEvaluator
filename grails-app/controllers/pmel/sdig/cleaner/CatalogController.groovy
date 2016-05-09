package pmel.sdig.cleaner

import static grails.async.Promises.*

class CatalogController {
    static scaffold = Catalog
    AsyncIngestService asyncIngestService
    IngestService ingestService
    TreeService treeService
    AsyncTreeService asyncTreeService
    DataCrawlService dataCrawlService
    AsyncDataCrawlService asyncDataCrawlService
    CleanService cleanService
    AsyncCleanService asyncCleanService
    MasterService masterService
    AsyncMasterService asyncMasterService
    CountService countService
    AsyncCountService asyncCountService
    def uddc() {
        def catid= params.catid
        long id = Long.valueOf(catid).longValue()
        Catalog cat = Catalog.get(id)
        Map<String, String> uddc = new HashMap<String, String>();
        if ( cat ) {
            if ( cat.leafNodeReferences ) {
                cat.leafNodeReferences.each{LeafNodeReference leafNodeReference ->
                    def url = leafNodeReference.url
                    LeafDataset ld = LeafDataset.findByUrl(url)
                    String threddsid = leafNodeReference.getThreddsid()
                    String cathtml = cat.url.replace(".xml", ".html")
                    String uddcurl = url.replace("dodsC", "uddc")
                    uddcurl = uddcurl + "?catalog=" + cathtml + "&dataset=" + threddsid
                    uddc.put(url, uddcurl)
                }
            }
            render view: "uddc", model: [uddc: uddc]
        }
    }
    def bad() {
        def catid= params.catid
        long id = Long.valueOf(catid).longValue()
        List<LeafDataset> datasets = new ArrayList<LeafDataset>();
        Catalog cat = Catalog.get(id)
        if ( cat ) {
            if ( cat.leafNodeReferences ) {
                cat.leafNodeReferences.each{LeafNodeReference leafNodeReference ->
                    def url = leafNodeReference.url
                    LeafDataset ld = LeafDataset.findByUrl(url)
                    if ( !ld.error.equals("none") || ( ld.badNetCDFVariables && ld.badNetCDFVariables.size() > 0 ) ) {
                        datasets.add(ld)
                    }
                }
            }



            render view: "bad", model: [datasets: datasets]
        } else {
            flash.message = "No catalog found."
        }
    }
    def top() {
        def catalogs = Catalog.findAllByParent("none")
        render view: "top", model: [catalogs: catalogs]
    }
    def clean() {
        def url = params.url
        def parent = params.parent
        try {

            //TODO check to see if the data crawl is done and either start it or return an error.
            flash.message = "Cleaning started..."

            cleanService.clean(parent, url) //.onComplete{log.debug("Completed clean for catalog.")}
            countService.countChildLeaves(parent, url)

            redirect([controller: "catalog", action: "index"])

        } catch (Exception e) {
            log.error("Error during cleaning step for "+parent+" and "+url)
            log.error("Error message: "+e)
        }

    }
    def enter() {

        try {
            def url = params.url
            if (url) {

                if ( url.endsWith(".html") ) url = url.replace(".html", ".xml")
                // This is a crawl from the root.
                // If you want to crawl a sub-catalog, you need somehow define where it is attached...
                def parent = params.parent
                def root = false
                if (!parent) {
                    root = true
                    parent = "none"
                }



                Catalog catalog = Catalog.findByParentAndUrl(parent, url)

                if (!catalog) {
                    catalog = new Catalog([parent: parent, root: true, url: url, status: Catalog.Status.TREE_CRAWL_STARTED])
                    catalog.save(flush: true)
                    try {

                        asyncMasterService.fullCrawl(parent, url).onComplete {log.debug("Full tree crawl, data crawl and clean finished.")}


                    } catch (Exception e) {
                        log.error("Full crawl error "+e.toString())
                    }

                    flash.message = "Catalog evaluation has started... Page will update every 5 seconds."
                    render view: "enter", model: [catalogInstance: catalog]
                    return

                } else {
                    if (catalog.status == Catalog.Status.TREE_CRAWL_STARTED) {
                        flash.message = "Performing the first step.  Collecting the catalog XML ..."
                        render view: "enter", model: [catalogInstance: catalog]
                        return
                    } else if (catalog.status == Catalog.Status.TREE_CRAWL_FINISHED) {
                        flash.message = "XML Collected. Staring the second step..."
                        render view: "enter", model: [catalogInstance: catalog]
                        return
                    } else if (catalog.status == Catalog.Status.DATA_CRAWL_STARTED) {
                        // Finished or failed
                        int done =LeafNodeReference.countByCatalogAndStatus(catalog, LeafNodeReference.Status.CRAWL_FINISHED) + LeafNodeReference.countByCatalogAndStatus(catalog, LeafNodeReference.Status.CRAWL_FAILED)
                        int total = LeafNodeReference.countByCatalog(catalog)
                        flash.message = "Performing the second step. Collecting the netCDF metadata... " + done + " out of "+ total + " data sets have been scanned."
                        render view: "enter", model: [catalogInstance: catalog]
                        return
                    } else if (catalog.status == Catalog.Status.DATA_CRAWL_FINISHED) {
                        flash.message = "netCDF metadata collection finished.  Starting the third step..."
                        render view: "enter", model: [catalogInstance: catalog]
                        return
                    } else if (catalog.status == Catalog.Status.CLEAN_STARTED) {
                        flash.message = "Staring the final step of evaluating the metadata."
                        render view: "enter", model: [catalogInstance: catalog]
                        return
                    } else if ( catalog.status == Catalog.Status.DOWNLOAD_FAILED ) {
                        flash.error = "This catalog could not be downloaded or is not valid"
                        catalog.delete(flush: true)
                        render view: "enter"
                    } else if (catalog.status == Catalog.Status.CLEAN_FINISHED) {
                        redirect controller: "rubric", action: "chart", params: [url: catalog.getUrl(), parent: catalog.getParent()]
                        return
                    }

                }

            }
        } catch (Exception e) {
            flash.message = e.toString()
        }
    }
    def reEnter() {
        def url = params.url
        flash.message = "Deleted "+url+" and its children."
        if (url) {

            def parent = params.parent
            if (!parent) parent = "none"
            try {
                Catalog catalogInstance = Catalog.findByParentAndUrl(parent, url)
                if ( catalogInstance ) {
                    ingestService.deleteAll(catalogInstance)
                    catalogInstance.delete(flush: true)
                }
                Catalog nc = new Catalog([parent: parent, root: true, url: url, status: Catalog.Status.TREE_CRAWL_STARTED])
                nc.save(flush: true)
                try {

                    asyncMasterService.fullCrawl(parent, url).onComplete {log.debug("Full tree crawl, data crawl and clean finished.")}


                } catch (Exception e) {
                    log.error("Full crawl error "+e.toString())
                }

                flash.message = "Catalog evaluation has started... Page will update every 5 seconds."
                render view: "enter", model: [catalogInstance: nc]
                return
            } catch (Exception e) {
                log.error("Ingest failed. " + e.message)
            }
        }
    }
    def deleteAll() {
        def url = params.url
        flash.message = "Deleted "+url+" and its children."
        if (url) {

            def parent = params.parent
            if (!parent) parent = "none"

            try {
                Catalog catalogInstance = Catalog.findByParentAndUrl(parent, url)
                if ( catalogInstance ) {
                    ingestService.deleteAll(catalogInstance)
                    catalogInstance.delete()
                }
            } catch (Exception e) {
                log.error("Ingest failed. " + e.message)
            }
        }
        render controller: "catalog", view: "index"
    }
//    def evaluate() {
//
//        def url = params.url
//        if (url) {
//
//            def parent = params.parent
//            if ( !parent ) parent = "none"
//
//            try {
//                Catalog catalogInstance = Catalog.findByParentAndUrl(parent, url)
//
//                if (!catalogInstance) {
//
//                    catalogInstance = new Catalog([url: url, parent: parent, status: Catalog.Status.TREE_CRAWL_STARTED])
//
//                    catalogInstance.save(failOnError: true)
//
//                    log.debug("Staring ingest on "+url+" with parent "+parent)
//
//                    asyncTreeService.treeCrawl(parent, url).onComplete{log.debug("On complete for parent catalog ingest just fired.")}
////                    catalogIngestService.ingest(parent, url)
//
//                    flash.message = "Catalog ingest has started..."
//
//                    return
//
//                } else if (catalogInstance.status == Catalog.Status.TREE_CRAWL_STARTED ) {
//                    flash.message = "Found a catalog..."
//                    redirect(controller: "catalog", action: "show", params: [id: catalogInstance.id])
//                } else {
//                    flash.message = "Catalog crawl is complete..."
//                    redirect(controller: "catalog", action: "show", params: [id: catalogInstance.id])
//                }
//            } catch (Exception e) {
//                log.error("Ingest failed. "+e.message)
//            }
//        }
//    }
    def crawldata() {

        String parent = params.parent
        String url = params.url

        def catalog  = Catalog.findByParentAndUrl(parent, url)

        log.debug("looking for "+parent+" and "+url)
        // Do the entire catalog in only one thread to avoid overwhelming the remote TDS

        if ( catalog ) {

            flash.message = "Data crawl started..."
            asyncDataCrawlService.datacrawl(parent, url)


            //.onComplete {log.debug("Final onComplete called for "+ url)}

            long id = catalog.id
            redirect([controller: "catalog", action: "show", params: [id: id]])

        } else {
            flash.message = "No such catalog found. url="+url+" parent="+parent
            redirect([controller: "catalog", action: "index"])
        }

    }
    def showalldatasets() {
        ingestService.logdatasets()
        redirect(controller: "catalog", action: "index")

    }

}
