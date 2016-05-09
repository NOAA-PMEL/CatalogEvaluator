package pmel.sdig.cleaner

class GeoAxisX {
	
	String type
	String units
	String name
	String title
	int dimensions
	boolean regular
	double min
	double max
	double delta
	long size
	static belongsTo = [netCDFVariable: NetCDFVariable]

    static constraints = {	
		
    }

    @Override
    public String toString() {
        "GeoX: "+"min: "+min+" max: "+max+" size: "+size+" regular: "+regular+" delta: "+delta
    }

    public String summary() {
        type+min+max+size+delta
    }
	
}
