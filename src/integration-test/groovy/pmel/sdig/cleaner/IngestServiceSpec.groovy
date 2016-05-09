package pmel.sdig.cleaner


import grails.test.mixin.integration.Integration
import grails.transaction.*
import spock.lang.*

@Integration
@Rollback
class IngestServiceSpec extends Specification {

    IngestService ingestService
    def setup() {
    }

    def cleanup() {
    }

    void "One URL with no data"() {
        given: "A bad url"
           def url = "http://dods.ndbc.noaa.gov:8080/thredds/dodsC/oceansites/DATA/T2N165E/OS_T2N165E_PM608A-20060710_R_POS.nc"
           LeafNodeReference lnr = new LeafNodeReference()
           lnr.setUrl(url)
           lnr.setUrlPath(url)
        when: "Ingested"
            ingestService.ingestDataSource(lnr)
        then: "there is an error sp1ecified"
           LeafDataset dataset = LeafDataset.findByUrl(url)
        expect:"fix me"
            dataset.error != "none"
    }
}
