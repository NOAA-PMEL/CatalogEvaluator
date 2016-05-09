package pmel.sdig.cleaner

import grails.async.DelegateAsync
import grails.async.Promise
import grails.async.Promises
import grails.transaction.Transactional

@Transactional
class AsyncDataCrawlService {

    @DelegateAsync DataCrawlService dataCrawlService
    Promise chainedDataCrawl(String parent, String url) {
        Promises.task {
            dataCrawlService.chainedDataCrawl(parent, url)
        }
    }

}
