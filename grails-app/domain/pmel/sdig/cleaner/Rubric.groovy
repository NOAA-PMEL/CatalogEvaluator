package pmel.sdig.cleaner

class Rubric {

    int leaves = 0
    int badLeaves = 0
    int aggregated
    int shouldBeAggregated
    int services
    int totalServices
    Date dateCreated
    Date lastUpdated

    static belongsTo = [catalog: Catalog]

    static hasMany = [missingServices: String]

    static constraints = {
        missingServices nullable: true
    }
}
