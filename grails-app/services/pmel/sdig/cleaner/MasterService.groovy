package pmel.sdig.cleaner

import grails.transaction.Transactional
import org.hibernate.SessionFactory

class MasterService {

    TreeService treeService
    DataCrawlService dataCrawlService
    CleanService cleanService
    CountService countService

    static transactional = false
    SessionFactory sessionFactory
    def fullCrawl(String parent, String url) {
        try {
            Catalog.withNewSession {
                treeService.treeCrawl(parent, url)
                sessionFactory.currentSession.flush()
            }
            log.debug("On complete for parent catalog ingest just fired. Starting data crawl...")
            Catalog.withNewSession {
                dataCrawlService.dataCrawl(parent, url)
                sessionFactory.currentSession.flush()
            }
            log.debug("On complete for data crawl. Starting clean...")
            Catalog.withNewSession {
                cleanService.clean(parent, url)
                sessionFactory.currentSession.flush()
            }
            log.debug("Completed clean for catalog. Finalizing leaf counts.")
            Catalog.withNewSession {
                countService.countChildLeaves(parent, url)
                sessionFactory.currentSession.flush()
            }
            log.debug("Leaves counted.")
        } catch (Exception e) {
            log.error("Full crawl error "+e.toString())
        }
    }
}
