package pmel.sdig.cleaner

class BadNetCDFVariable {
    String name;
    String title;
    String error = "none";
    static belongsTo = [leafDataSet: LeafDataset]
    static constraints = {
        title nullable: true
        name nullable: true
    }
    static mapping = {
        error type: 'text'
    }
}
