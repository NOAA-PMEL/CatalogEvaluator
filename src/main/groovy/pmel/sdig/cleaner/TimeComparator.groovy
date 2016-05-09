package pmel.sdig.cleaner

/**
 * Created by rhs on 11/20/15.
 */
class TimeComparator implements Comparator<LeafDataset> {
    @Override
    public int compare(LeafDataset o1, LeafDataset o2) {
        List<NetCDFVariable> variablesO1 = o1.getNetCDFVariables();
        List<NetCDFVariable> variablesO2 = o2.getNetCDFVariables();
        if ( variablesO1 == null || variablesO1.size() <= 0 ) return -1;
        if ( variablesO2 == null || variablesO1.size() <= 0 ) return 1;

        NetCDFVariable v1 = variablesO1.iterator().next();
        NetCDFVariable v2 = variablesO2.iterator().next();
        TimeAxis t1 = v1.getTimeAxis();
        TimeAxis t2 = v2.getTimeAxis();
        if ( t1 == null ) return -1;
        if ( t2 == null ) return 1;
        double startTime1 = t1.getMin();
        double startTime2 = t2.getMin();
        if ( startTime1 < startTime2 ) {
            return -1;
        } else if ( startTime1 > startTime2 ) {
            return 1;
        } else {
            return 0;
        }
    }
}
