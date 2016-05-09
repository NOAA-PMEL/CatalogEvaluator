package pmel.sdig.cleaner

import grails.async.DelegateAsync
import grails.async.Promise
import grails.async.Promises

class AsyncIngestService {

    @DelegateAsync IngestService ingestService

}
