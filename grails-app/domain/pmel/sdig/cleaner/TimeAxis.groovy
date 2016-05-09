package pmel.sdig.cleaner

class TimeAxis {
	
	String calendar
	String name
	String title
	String units
	// These will always, always be full ISO date/time Strings...
	String start
	String end
	// A string of the form PyYmMwWdDThHmMsS (weeks will only appear by itself)
	// The period of the entire time axis
	String period
	// Same as above, the period between two time steps.
	String delta
	String position
	boolean climatology
	long size

    // For convenience keep the double min and max value
    double min
    double max

	static belongsTo = [netCDFVariable: NetCDFVariable]

    static constraints = {
        delta (nullable: true)
		period (nullable: true)
		position (nullable: true)
    }
}
