package pmel.sdig.cleaner

class VerticalAxis {

    String type
    String name
    String title
    String boundaryRef
    double min
    double max
    double delta
    long size
    String positive
    String units
    boolean contiguous
    boolean regular
    int elementSize
    double start

    List zvalues

    static hasMany = [zvalues: Zvalue]

    static belongsTo = [netCDFVariable: NetCDFVariable]

    static mapping = {
        zvalues cascade:'all-delete-orphan'
    }

    static constraints = {
        zvalues nullable: true
        type nullable: true
        name nullable: true
        title nullable: true
        boundaryRef nullable: true
        min nullable: true
        max nullable: true
        delta nullable: true
        size nullable: true
        positive nullable: true
        units nullable: true
        contiguous nullable: true
        regular nullable: true
        elementSize nullable: true
        start nullable: true
    }
}
