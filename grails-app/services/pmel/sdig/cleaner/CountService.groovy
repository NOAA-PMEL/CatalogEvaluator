package pmel.sdig.cleaner

import grails.transaction.Transactional

class CountService {

    static transactional = false

    def countChildLeaves(String parent, String url) {

        Catalog catalog = Catalog.findByParentAndUrl(parent, url)

        if (catalog.status != Catalog.Status.DOWNLOAD_FAILED ) {
            doCount(catalog)
        }

        log.debug("Finished counting leaves...")
    }
    def doCount(Catalog catalog){
        log.debug("Count leaves...")
        if (catalog.getSubCatalogs()) {
            catalog.getSubCatalogs().each {
                countChildLeaves(catalog.getUrl(), it.getUrl())
            }
        }
        Rubric rubric = catalog.getRubric()
        int good = 0
        int aggregated = 0;
        int bad = 0
        if (catalog.getSubCatalogs()) {
            catalog.subCatalogs.each {Catalog child ->
                Rubric childRubric = child.getRubric()
                aggregated = aggregated + childRubric.getAggregated()
                good = good + childRubric.getLeaves()
                bad = bad + childRubric.getBadLeaves()
            }
        }

        int mygood = rubric.getLeaves()
        int mybad = rubric.getBadLeaves()
        int myaggregated = rubric.getAggregated();
        rubric.setLeaves(mygood + good)
        rubric.setBadLeaves(mybad + bad)
        rubric.setAggregated(myaggregated + aggregated)
        if (!rubric.validate()) {
            rubric.errors.each {
                log.debug(it.toString())
            }
        } else {
            rubric.save()
        }
        if ( catalog.validate() ) {
            catalog.setStatus(Catalog.Status.CLEAN_FINISHED)
            Catalog.lock(10)
            catalog.save(flush: true)
        } else {
            catalog.errors.each {
                log.error(it.toString())
            }
        }
    }
}
