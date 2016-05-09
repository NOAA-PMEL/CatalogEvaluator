package pmel.sdig.cleaner

import grails.async.DelegateAsync
import grails.async.Promise
import grails.async.Promises
import grails.transaction.Transactional

@Transactional
class AsyncTreeService {

    @DelegateAsync TreeService treeService
    Promise chainedTreeCrawl(String parent, String url) {
        Promises.task {
            treeService.treeCrawl(parent, url)
        }
    }

}
