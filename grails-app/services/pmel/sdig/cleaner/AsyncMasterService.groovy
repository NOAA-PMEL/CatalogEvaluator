package pmel.sdig.cleaner

import grails.async.DelegateAsync
import grails.transaction.Transactional

@Transactional
class AsyncMasterService {

    @DelegateAsync MasterService masterService
}
