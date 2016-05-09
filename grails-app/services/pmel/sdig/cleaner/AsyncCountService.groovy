package pmel.sdig.cleaner

import grails.async.DelegateAsync
import grails.async.Promise
import grails.async.Promises
import grails.transaction.Transactional

@Transactional
class AsyncCountService {

    @DelegateAsync CountService countService
    Promise countChildLeaves(String parent, String url) {
        Promises.task {
            countService.countChildLeaves(parent, url)
        }
    }


}
