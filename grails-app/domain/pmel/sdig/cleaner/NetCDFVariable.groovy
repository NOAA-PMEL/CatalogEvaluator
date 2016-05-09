package pmel.sdig.cleaner

class NetCDFVariable {

    String name;
    String description;
    String title;
    String info;
    int rank;
    String unitsString;
    String hasMissingData;
    String dataType;
    String standardName;
    String longName;

    // ?? what to do about this: Projection projection;
    double lonMin;
    double lonMax;
    double latMin;
    double latMax;

    List stringAttributes

    static belongsTo = [leafDataSet: LeafDataset]

    static hasOne = [timeAxis: TimeAxis, geoAxisX: GeoAxisX, geoAxisY: GeoAxisY, verticalAxis: VerticalAxis]
    static hasMany = [stringAttributes: StringAttribute]

    static constraints = {
        stringAttributes nullable: true
        title nullable: true
        name nullable: true
        timeAxis nullable: true
        geoAxisX nullable: true
        geoAxisY nullable: true
        verticalAxis nullable: true
        unitsString nullable: true
        longName nullable: true
        standardName nullable: true
        dataType nullable: true
        description nullable: true
        hasMissingData nullable: true
        rank nullable: true
        info nullable: true

        lonMin nullable: true
        lonMax nullable: true
        latMin nullable: true
        latMax nullable: true
    }
    static mapping = {

        stringAttributes (cascade: 'all-delete-orphan')
        description type: 'text'

    }
}
