package pmel.sdig.cleaner

class StringAttribute {

    List values
    String name
    static hasMany = [values: String]
    static belongsTo = [netCDFVariable: NetCDFVariable]
    static mapping = {
        values type: 'text'
    }
    static constraints = {

        netCDFVariable (nullable: true)
        values (nullable: true)
        name (nullable: true)

    }
}
