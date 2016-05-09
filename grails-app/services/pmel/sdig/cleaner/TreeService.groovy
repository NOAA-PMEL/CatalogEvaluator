package pmel.sdig.cleaner

import grails.transaction.Transactional
import groovy.util.slurpersupport.NodeChild
import org.hibernate.SessionFactory
import org.springframework.web.context.request.RequestContextHolder
import org.xml.sax.SAXParseException
import thredds.catalog.InvAccess
import thredds.catalog.InvCatalog
import thredds.catalog.InvCatalogFactory
import thredds.catalog.InvDataset
import thredds.catalog.ServiceType

class TreeService {

    DataCrawlService dataCrawlService
    CleanService cleanService
    CountService countService
    SkipService skipService

    SessionFactory sessionFactory

    static transactional = false



    def treeCrawl(String parent, String url) {
        String sourceXml
        CatalogXml childXml
        Catalog child = Catalog.findByParentAndUrl(parent, url)

        def childParsedXml

        try {



            log.info("Processing "+ child.url + " with parent " + child.parent)
            log.debug("Found child catalog like this: " + child.url + " with status " + child.status)

            childXml = CatalogXml.findByUrl(url)

            def urlObject = url.toURL()

            String host = urlObject.getHost()
            int port = urlObject.getPort();


            if (urlObject)
                sourceXml = urlObject.text


            InvCatalogFactory factory = InvCatalogFactory.getDefaultFactory(false)
            InvCatalog childCatalog  = factory.readXML(url);

            log.debug("XML has been downloaded.")

            if (childXml) {
                if (!sourceXml.equals(childXml.xml)) {
                    childXml.xml = sourceXml;
                    // Save the new XML
                    childXml.save(failOnError: true)
                }
            } else {
                childXml = new CatalogXml()
                childXml.xml = sourceXml
                childXml.url = url
                childXml.save(failOnError: true)
            }

            def curls = findCatalogRefNodes(host, port, child.url, sourceXml)


                curls.each { String refurl ->
                    if (!skipService.skip(child.url, refurl)) {
                        def ref = new Catalog()
                        ref.setParentCatalog(child)
                        ref.setParent(child.url)
                        ref.setUrl(refurl)
                        ref.setStatus(Catalog.Status.CRAWL_NOT_STARTED)
                        child.addToSubCatalogs(ref)
                        log.debug("Added catalog ref: " + ref + " with url " + refurl)
                    }
                }


            log.debug("Sub-catalogs added.")
            if ( !childCatalog ) {
                child.delete(flush: true)
                log.debug("Failed to parse catalog...")
                throw new Exception("THREDDS cataog is unavialable or cannot be parsed.")
            }

            List<InvDataset> rootInvDatasets = childCatalog.getDatasets();

            def localDatasets = []
            localDatasets = findAccessDatasets(url, rootInvDatasets, localDatasets)
            def size = 0
            if ( localDatasets ) {
                size = localDatasets.size()
            }
            child.setLeafCount(size)

            boolean best = false
            if (localDatasets && size < 250) {

                localDatasets.each { LeafNodeReference lnr ->
                    if (lnr) {
                        if (lnr.isBestTimeseries) {
                            best = true;
                        }
                        if (lnr.url) {
                            child.addToLeafNodeReferences(lnr)
                            log.debug("Adding a leaf node with url " + lnr.url + " urlPath = " + lnr.urlPath)
                        }
                    }
                }
                child.status = Catalog.Status.TREE_CRAWL_FINISHED
            } else {
                if (size > 250) {
                    child.status = Catalog.Status.DO_NOT_CRAWL
                } else {
                    child.status = Catalog.Status.TREE_CRAWL_FINISHED
                }
            }
            // If it has a best time series, remove the children
            if ( best ) {
                child.subCatalogs.clear()
            }

            Catalog.lock(1)
            child.save(flush: true)
            log.debug("Catalog saved with FINISHED status")

            if ( child.subCatalogs ) {
                child.subCatalogs.each { Catalog subCatalog ->
                    log.debug("Now ingesting " + subCatalog.url + " from " + child.url)


                        subCatalog.setParent(child.url)
                        subCatalog.setParentCatalog(child)
                        subCatalog.setStatus(Catalog.Status.TREE_CRAWL_STARTED)
                        Catalog.lock(2)
                        subCatalog.save(flush: true)
                        treeCrawl(child.url, subCatalog.url)


                }
            }


        } catch (Exception e) {
            if ( !childXml || !sourceXml || !childParsedXml) {
                child.status = Catalog.Status.DOWNLOAD_FAILED
                child.save(flush: true)
            }

        }
    }
    def List<String> findCatalogRefNodes (String host, int port, String curl, String xml) {

        List<String> cref = new ArrayList<String>();
        def childParsedXml  = new XmlSlurper().parseText(xml)

        def refs = childParsedXml.'**'.findAll { node -> node.name() == "catalogRef" }

        refs.each { NodeChild node ->

            def attrs = node.attributes()

            def refurl = attrs.get('{http://www.w3.org/1999/xlink}href')


            if ( !refurl.startsWith("http") && host ) {
                if (refurl.startsWith("/")) {
                    // Starts with "/" so is relative to the root.
                    def path = refurl
                    refurl = "http://" + host
                    if (port != 80) {
                        refurl = refurl + ":" + port
                    }
                    refurl = refurl + path
                } else {
                    // No "/" so relative to the current catalog. Use +1 to keep the final "/"
                    refurl = curl.substring(0, curl.lastIndexOf("/")+1) + refurl
                }
            }
            if (refurl) {
                cref.add(refurl)
            } else {
                log.debug("This URL is not what it seems: "+refurl)
            }
        }

            return cref
    }
    def List<LeafNodeReference> findAccessDatasets(String url, List<InvDataset> invDatasets, List<LeafNodeReference> datasets) {
        for ( Iterator dsIt = invDatasets.iterator(); dsIt.hasNext(); ) {
            InvDataset ds = (InvDataset) dsIt.next();
            String catalogUrl = "";
            try {
                catalogUrl = ds.getCatalogUrl();
                catalogUrl = catalogUrl.substring(0, catalogUrl.indexOf("#"))
            } catch (Exception e) {
                // Shine on for now
            }
            // Only deal with data sets which are in this catalog --
            //    if they have access and are local include,
            //    if they have sub-datasets and are local continue searching

            if (catalogUrl.equals(url)) {
                if (ds.hasAccess()) {

                    def threddsid = ds.getID()
                    InvAccess access = ds.getAccess(ServiceType.OPENDAP);

                    if (access != null) {

                        String locationUrl = access.getStandardUrlName();
                        String urlPath = access.getUrlPath();
                        // Only collect local data sets.
                        if ( locationUrl ) {

                            boolean best = false
                            if ( ds.getName().toLowerCase().contains("best time") ) {
                                best = true
                            }

                            def newLNR = new LeafNodeReference([url: locationUrl, urlPath: urlPath, isBestTimeseries: best, status: LeafNodeReference.Status.CRAWL_NOT_STARTED])
                            newLNR.setUrl(locationUrl)
                            newLNR.setUrlPath(urlPath)
                            newLNR.setStatus(LeafNodeReference.Status.CRAWL_NOT_STARTED)
                            newLNR.setThreddsid(threddsid)
                            datasets << newLNR
                        }

                    }

                } else if (ds.hasNestedDatasets()) {
                    List<InvDataset> children = ds.getDatasets();
                    findAccessDatasets(url, children, datasets);
                }
            }

        }
        return datasets;
    }
}
