package pmel.sdig.cleaner

import grails.async.DelegateAsync
import grails.async.Promise
import grails.async.Promises
import grails.transaction.Transactional

@Transactional
class AsyncCleanService {

    @DelegateAsync CleanService cleanService

    Promise clean(String parent, String url) {
        Promises.task {
            cleanService.clean(parent, url)
        }
    }


}
