package pmel.sdig.cleaner

import grails.transaction.Transactional
import groovy.util.slurpersupport.NodeChild
import org.jdom2.Document
import org.jdom2.Element
import org.jdom2.Namespace
import org.jdom2.Parent
import org.jdom2.output.Format
import org.jdom2.output.XMLOutputter
import org.jdom2.util.IteratorIterable
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Period
import org.joda.time.format.ISOPeriodFormat
import org.joda.time.format.PeriodFormatter
import pmel.sdig.cleaner.TimeComparator
import pmel.sdig.cleaner.util.Util
import thredds.catalog.*
import ucar.ma2.DataType
import ucar.nc2.Attribute
import ucar.nc2.constants.FeatureType
import ucar.nc2.dataset.CoordinateAxis
import ucar.nc2.dataset.CoordinateAxis1D
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.nc2.dataset.CoordinateAxis2D
import ucar.nc2.dt.GridCoordSystem
import ucar.nc2.dt.GridDataset
import ucar.nc2.dt.GridDatatype
import ucar.nc2.ft.FeatureDatasetFactoryManager
import ucar.nc2.time.CalendarDate
import ucar.nc2.time.CalendarDateRange
import ucar.nc2.time.CalendarDateUnit
import ucar.nc2.units.TimeUnit

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.regex.Pattern

@Transactional
class IngestService {

    def logdatasets() {
        Catalog catalog = Catalog.findByParent("none")
        printDatasets(catalog)
    }
    def printDatasets(Catalog catalog) {
        if ( catalog.leafNodeReferences ) {
            catalog.leafNodeReferences.each{
                LeafDataset d = LeafDataset.findByUrl(it.getUrl())
                log.debug("Dataset "+d.getUrl()+" has "+d.getNetCDFVariables().size()+" variables.")
            }
        }
        if ( catalog.subCatalogs ) {
            catalog.subCatalogs.each{Catalog child ->
                printDatasets(child)
            }
        }
    }
    def deleteAll(Catalog catalog) {
        CatalogXml xml = CatalogXml.findByUrl(catalog.url);
        if ( xml ) {
            xml.delete()
        }
        if ( catalog.leafNodeReferences ) {
            catalog.leafNodeReferences.each {
                LeafDataset d = LeafDataset.findByUrl(it.url)
                if (d) {
                    d.delete()
                }

            }
        }
        if ( catalog.subCatalogs ) {
            catalog.subCatalogs.each {Catalog child ->
                deleteAll(child)
            }
        }
    }
    def ingestDataSource(LeafNodeReference lnr) {

        def url = lnr.url


        log.info("Ingesting "+ lnr.url)

        DateTime now = new DateTime()
        LeafDataset dataset = LeafDataset.findByUrl(url)
        if (!dataset) {
            dataset = new LeafDataset(url: url)
            dataset.setUrl(url)
        }
        dataset.setStatus(LeafDataset.Status.CRAWL_STARTED)
        dataset.setCrawlStartTime(now.toString());
        dataset.setUrlPath(lnr.getUrlPath())
        def hash = getDigest(url)
        dataset.setHash(hash)

        dataset.save(failOnError: true)

        // Is it a netCDF data source?

        // TODO catch exepctions and keep going...

        Formatter error = new Formatter();

        GridDataset gridDs;
        try {
            gridDs = (GridDataset) FeatureDatasetFactoryManager.open(FeatureType.GRID, url, null, error);
        } catch (Exception e) {
            dataset.setError("Failed to get data with ${e.getMessage()}")
            now = new DateTime();
            dataset.setCrawlEndTime(now.toString())
            dataset.setStatus(LeafDataset.Status.CRAWL_FAILED)
            lnr.setStatus(LeafNodeReference.Status.CRAWL_FAILED);
            if ( !lnr.validate() ) {
                log.debug("LNR did not validate for error save.")
            } else {
                lnr.save(failOnError: true, flush: true)
            }
            if ( !dataset.validate() ) {
                log.debug("Data set did not validate for error save.")
            } else {
                dataset.save(failOnError: true, flush: true)
            }
        }

        if (gridDs != null) {



            List<Attribute> globals = gridDs.getGlobalAttributes();
            // Get the DRS information

            String title = url;
            for (Iterator iterator = globals.iterator(); iterator.hasNext();) {
                Attribute attribute = (Attribute) iterator.next();
                if (attribute.getShortName().equals("title")) {
                    title = attribute.getStringValue()
                }
            }

            List<GridDatatype> grids = gridDs.getGrids();

            if ( grids.size() == 0 ) {
                dataset.setError("This data set was examined using known netCDF conventions. No data variables were found.")
            }
            if (url.equals("http://dods.ndbc.noaa.gov:8080/thredds/dodsC/oceansites/DATA/T2N165E/OS_T2N165E_PM608A-20060710_R_POS.nc")) {
                log.debug("Stop here a minute.")
            }

            for (Iterator iterator = grids.iterator(); iterator.hasNext();) {
                GridDatatype gridDatatype = (GridDatatype) iterator.next();

                def stringAttributes = []
                log.debug("Scanning for text attributes...")
                gridDatatype.getAttributes().each {Attribute attr ->
                    if (attr.getDataType() == DataType.STRING || attr.getDataType() == DataType.CHAR ) {
                        String name = attr.getShortName()
                        if ( name.startsWith("_")) name="coord"
                        String[] values = attr.getValues().get1DJavaArray(java.lang.String);
                        if ( name && values ) {
                            log.debug("name "+name+" and values "+values+" was added.")
                            StringAttribute sa = new StringAttribute()
                            sa.setName(name)
                            sa.setValues(Arrays.asList(values))
                            stringAttributes.add(sa)
                        } else {
                            log.debug("name "+name+" or values "+values+" was kaput.")
                        }

                    }
                }
                log.debug("Attributes added")
                def variableError = "none"
                // The variable basics
                String vname = gridDatatype.getShortName()

                int rank = gridDatatype.getRank()

                //TODO do I need to get the attributes and find the long_name myself?

                String vtitle = gridDatatype.getDescription()
                if (vtitle == null) {
                    vtitle = vname;
                }
                String vhash = getDigest(url + ":" + gridDatatype.getDescription())


                GridCoordSystem gcs = gridDatatype.getCoordinateSystem();

                // Axes are next...
                long tIndex = -1;
                long zIndex = -1;
                TimeAxis tAxis = null
                if (gcs.hasTimeAxis()) {

                    if (gcs.hasTimeAxis1D()) {
                        CoordinateAxis1DTime time = gcs.getTimeAxis1D();
                        CalendarDateRange range = time.getCalendarDateRange();

                        // Get the basics
                        String start = range.getStart().toString();
                        String end = range.getEnd().toString();
                        long size = time.getSize();
                        tIndex = size / 2;
                        if (tIndex <= 0) tIndex = 1;
                        String units = time.getUnitsString();
                        Attribute cal = time.findAttribute("calendar");
                        String calendar = "standard";
                        if (cal != null) {
                            calendar = cal.getStringValue(0);
                        }
                        String shortname = time.getShortName();
                        String timetitle = time.getFullName();

                        // Figure out the delta (as a period string) and where the time is marked (beginning, middle, or end of the period
                        double[] tb1 = time.getBound1();
                        double[] tb2 = time.getBound2();
                        double[] times = time.getCoordValues();

                        CalendarDateUnit cdu = CalendarDateUnit.of(calendar, units);
                        Period p0 = null;
                        String position0 = getPosition(times[0], tb1[0], tb2[0]);
                        boolean regular = true;
                        boolean constant_position = true;
                        regular = time.isRegular();



                        if (times.length > 1) {
                            List<CalendarDate> dates = time.getCalendarDates()
                            for (int i = 0; i < dates.size() - 1; i++) {
                                if (dates.get(i).getMillis() >= dates.get(i + 1).getMillis()) {
                                    variableError = "Time axis is not monotonic at $i=" + dates.get(i) + " ${i+1}=" + dates.get(i + 1)
                                }
                            }
                            if ( variableError != "none") {
                                log.error(variableError);
                            }
                            try {
                                TimeUnit tu = time.getTimeResolution();
                                double du = tu.getValue();
                                String u = tu.getUnitString();
                                if (u.contains("sec")) {
                                    // More than a minute and less than an hour
                                    // How many minutes?
                                    if (du > 60 && du < 3600) {
                                        int floor = Math.floor(du / 60.0d);
                                        int ceil = Math.ceil(du / 60.0d);
                                        if (floor == ceil) {
                                            p0 = new Period(0, 0, 0, 0, 0, floor, 0, 0);
                                        } else {
                                            p0 = new Period(0, 0, 0, 0, 0, 0, du as int, 0);
                                        }
                                    }

                                } else if (u.contains("hour")) {
                                    for (int d = 0; d < 27; d++) {
                                        if (du < 23.5 * d && du < 23.5 * d + 1) {
                                            // Period(int years, int months, int weeks, int days, int hours, int minutes, int seconds, int millis)
                                            p0 = new Period(0, 0, 0, d, 0, 0, 0, 0);
                                        }
                                    }
                                    if (p0 == null) {
                                        if (du > 28 * 24 && du < 33 * 24) {
                                            p0 = new Period(0, 1, 0, 0, 0, 0, 0, 0);
                                        }
                                    }

                                } else if (u.contains("day")) {
                                    if (du < 1) {
                                        int hours = du * 24.0d
                                        p0 = new Period(0, 0, 0, 0, hours, 0, 0, 0)
                                    } else {
                                        p0 = new Period(0, 0, 0, du, 0, 0, 0, 0)
                                    }

                                } else if (u.contains("week")) {
                                    log.error("units string with week as units")
                                } else if (u.contains("year")) {
                                    log.error("units string with year as units")
                                }

                            } catch (Exception e) {
                                // bummer
                            }
                        } else {


                        }
                        //						for (int tindx = 0; tindx < tb1.length; tindx++ ) {
                        //							String position = getPosition(times[tindx], tb1[tindx], tb2[tindx]);
                        //							Period p = getPeriod(cdu, tb1[tindx], tb2[tindx]);
                        //							if ( !position0.equals(position) ) {
                        //								constant_position = false;
                        //							}
                        //							if ( !p.equals(p0) ) {
                        //								regular = false;
                        //							}
                        //						}


                        PeriodFormatter pf = ISOPeriodFormat.standard();
                        Period period = getPeriod(cdu, times[0], times[times.length - 1]);

                        tAxis = new TimeAxis();
                        if (p0 != null) {
                            tAxis.setDelta(pf.print(p0));
                        }
                        tAxis.setPeriod(pf.print(period));
                        tAxis.setStart(start);
                        tAxis.setEnd(end);
                        if (start.contains("0000") && end.contains("0000")) {
                            tAxis.setClimatology(true);
                        } else {
                            tAxis.setClimatology(false);
                        }
                        tAxis.setSize(size);
                        tAxis.setUnits(units);
                        tAxis.setCalendar(calendar);
                        tAxis.setTitle(title);
                        tAxis.setName(shortname);

                        if (regular) {
                            tAxis.setDelta(pf.print(p0));
                        }
                        if (constant_position) {
                            tAxis.setPosition(position0);
                        }


                    } else {
                        // TODO 2D Time Axis
                    }
                }

                CoordinateAxis xca = gcs.getXHorizAxis();
                GeoAxisX xAxis = null
                if (xca instanceof CoordinateAxis1D) {
                    CoordinateAxis1D x = (CoordinateAxis1D) xca;
                    xAxis = new GeoAxisX()
                    xAxis.setType("x");
                    xAxis.setTitle(x.getFullName());
                    xAxis.setName(x.getShortName());
                    xAxis.setUnits(x.getUnitsString());
                    xAxis.setRegular(x.isRegular());
                    if (x.isRegular()) {
                        xAxis.setDelta(x.getIncrement());
                    }
                    xAxis.setMin(x.getMinValue());
                    xAxis.setMax(x.getMaxValue());
                    xAxis.setSize(x.getSize());
                    xAxis.setDimensions(1)

                } else if (xca instanceof CoordinateAxis2D) {
                    CoordinateAxis2D x = (CoordinateAxis2D) xca;
                    xAxis = new GeoAxisX()
                    xAxis.setType("x");
                    xAxis.setTitle(x.getFullName());
                    xAxis.setName(x.getShortName());
                    xAxis.setUnits(x.getUnitsString());
                    xAxis.setRegular(false);
                    xAxis.setDimensions(2);
                    xAxis.setMin(x.getMinValue());
                    xAxis.setMax(x.getMaxValue());
                    xAxis.setSize(x.getSize());
                }
                GeoAxisY yAxis = null;
                CoordinateAxis yca = gcs.getYHorizAxis();
                if (yca instanceof CoordinateAxis1D) {
                    CoordinateAxis1D y = (CoordinateAxis1D) yca;
                    yAxis = new GeoAxisY();
                    yAxis.setType("y");
                    yAxis.setTitle(y.getFullName());
                    yAxis.setName(y.getShortName());
                    yAxis.setUnits(y.getUnitsString());
                    yAxis.setRegular(y.isRegular());
                    if (y.isRegular()) {
                        yAxis.setDelta(y.getIncrement());
                    }
                    yAxis.setMin(y.getMinValue());
                    yAxis.setMax(y.getMaxValue());
                    yAxis.setSize(y.getSize());
                    yAxis.setDimensions(1);

                } else {
                    CoordinateAxis2D y = (CoordinateAxis2D) yca;
                    yAxis = new GeoAxisY();
                    yAxis.setType("y");
                    yAxis.setTitle(y.getFullName());
                    yAxis.setName(y.getShortName());
                    yAxis.setUnits(y.getUnitsString());
                    yAxis.setRegular(false);
                    yAxis.setMin(y.getMinValue());
                    yAxis.setMax(y.getMaxValue());
                    yAxis.setSize(y.getSize());
                    yAxis.setDimensions(2);
                }
                CoordinateAxis1D z = gcs.getVerticalAxis();
                VerticalAxis zAxis = null
                if (z != null) {
                    // Use the first z. It's probably more interesting.
                    zIndex = 1;
                    zAxis = new VerticalAxis()
                    zAxis.setSize(z.getSize());
                    zAxis.setType("z");
                    zAxis.setTitle(z.getFullName());
                    zAxis.setName(z.getShortName());
                    zAxis.setMin(z.getMinValue());
                    zAxis.setMax(z.getMaxValue());
                    zAxis.setRegular(z.isRegular());
                    zAxis.setUnits(z.getUnitsString());
                    if (zAxis.isRegular()) {
                        zAxis.setDelta(z.getIncrement());
                    }
                    double[] v = z.getCoordValues();
                    for (int j = 0; j < v.length; j++) {
                        Zvalue zv = new Zvalue();
                        zv.setValue(v[j]);
                        zAxis.addToZvalues(zv);
                    }

                    zAxis.setPositive(z.getPositive());

                }


                String vunits = gridDatatype.getUnitsString();


                NetCDFVariable variable = new NetCDFVariable()

                //([url: url, name: vname, title: vtitle, rank: rank, geoAxisX: xAxis, geoAxisY: yAxis, verticalAxis: zAxis, timeAxis: tAxis, error: variableError, stringAttributes: stringAttributes])
                // TODO Do I need this?   ... variable.setUrl(url)
                if (vunits) variable.setUnitsString(vunits)
                variable.setName(vname)
                variable.setTitle(vtitle)
                variable.setRank(rank)
                variable.setGeoAxisX(xAxis)
                variable.setGeoAxisY(yAxis)
                if ( zAxis ) variable.setVerticalAxis(zAxis)
                if ( tAxis ) variable.setTimeAxis(tAxis)

                if ( stringAttributes ) {
                    for (int i = 0; i < stringAttributes.size(); i++) {
                        variable.addToStringAttributes(stringAttributes.get(i))
                    }
                }

                if (variableError == "none") {
                    log.debug("Adding variable: " + vtitle + " to " + url)
                    dataset.addToNetCDFVariables(variable)
                } else {
                    log.debug("Adding BAD variable: " + vtitle + " to " + url+" with error "+variableError)
                    def bad = new BadNetCDFVariable()
                    bad.setName(variable.getName())
                    bad.setTitle(variable.getTitle())
                    bad.setError(variableError)
                    dataset.addToBadNetCDFVariables(bad)
                }

                dataset.netCDFVariables.each{
                    // Necessary because of a grails bug cascading saves...
                    // Does not work even with the cascade mapping set in the parent
                    it.stringAttributes.each {
                        it.save(failOnError: true)
                    }
                    it.save(failOnError: true)
                }

                dataset.save(failOnError: true)

            }


            if (dataset.validate()) {
                log.debug("data collection complete for "+url)
                DateTime end = new DateTime()
                dataset.setCrawlEndTime(end.toString())
                dataset.setStatus(LeafDataset.Status.CRAWL_FINISHED)
                lnr.setStatus(LeafNodeReference.Status.CRAWL_FINISHED);
                lnr.save(failOnError: true)
                dataset.save(failOnError: true)
                log.debug("Final data set save complete for "+url)
            } else {
                dataset.errors.allErrors.each {
                    log.debug(it)
                }
            }

        } else {
            def dserror = "Data set was examined using known netCDF conventions and no data variables were found."
            if ( error.toString().length() > 0 ) {
                dserror = dserror + " NetCDF error = " + error.toString()
            }
            dataset.setError(dserror)
            now = new DateTime();
            dataset.setCrawlEndTime(now.toString())
            dataset.setStatus(LeafDataset.Status.CRAWL_FAILED)
            lnr.setStatus(LeafNodeReference.Status.CRAWL_FAILED);
            if ( !lnr.validate() ) {
                log.debug("LNR did not validate for error save.")
            } else {
                lnr.save(failOnError: true, flush: true)
            }
            if ( !dataset.validate() ) {
                log.debug("Data set did not validate for error save.")
            } else {
                dataset.save(failOnError: true, flush: true)
            }
        }

        // Is it an ESGF catalog or data set?
    }

    def Period getPeriod(CalendarDateUnit cdu, double t0, double t1) {
        CalendarDate cdt0 = cdu.makeCalendarDate(t0);
        CalendarDate cdt1 = cdu.makeCalendarDate(t1);
        DateTime dt0 = new DateTime(cdt0.getMillis()).withZone(DateTimeZone.UTC);
        DateTime dt1 = new DateTime(cdt1.getMillis()).withZone(DateTimeZone.UTC);

        return new Period(dt0, dt1);
    }
    def String getPosition(double t, double tb1, double tb2) {
        String position = null;
        double c1 = tb1 - t;
        double ca1 = Math.abs(c1);

        double delta = 0.00001d;

        if ( c1 < delta ) {
            position = "beginning"
        }

        double c2 = t - tb2;
        double ca2 = Math.abs(c2);

        if ( ca2 < delta ) {
            position = "end";
        }
        if ( Math.abs(( tb1 + ((tb2 - tb1)/2.0d) ) - t) < delta ) {
            position = "middle"
        }
        return position;
    }
    def static String getDigest(String url) {
        MessageDigest md;
        StringBuffer sb = new StringBuffer();
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(url.getBytes());
            byte[] digest = md.digest();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            System.err.println(e.getMessage())
        }
        return sb.toString();


    }

    def List<LeafNodeReference> findAccessDatasets(String url, List<InvDataset> invDatasets, List<LeafNodeReference> datasets) {
        for ( Iterator dsIt = invDatasets.iterator(); dsIt.hasNext(); ) {
            InvDataset ds = (InvDataset) dsIt.next();
            String catalogUrl = "";
            try {
                catalogUrl = ds.getCatalogUrl();
                catalogUrl = catalogUrl.substring(0, catalogUrl.indexOf("#"))
            } catch (Exception e) {
                // Shine on for now
            }
            // Only deal with data sets which are in this catalog --
            //    if they have access and are local include,
            //    if they have sub-datasets and are local continue searching

            if (catalogUrl.equals(url)) {
                if (ds.hasAccess()) {
                    InvAccess access = ds.getAccess(ServiceType.OPENDAP);

                    if (access != null) {



                        String locationUrl = access.getStandardUrlName();
                        String urlPath = access.getUrlPath();
                        // Only collect local data sets.
                        if ( locationUrl ) {

                            def newLNR = new LeafNodeReference([url: locationUrl, urlPath: urlPath, status: LeafNodeReference.Status.CRAWL_NOT_STARTED])
                            newLNR.setUrl(locationUrl)
                            newLNR.setUrlPath(urlPath)
                            newLNR.setStatus(LeafNodeReference.Status.CRAWL_NOT_STARTED)
                            datasets << newLNR
                        }

                    }

                } else if (ds.hasNestedDatasets()) {
                    List<InvDataset> children = ds.getDatasets();
                    findAccessDatasets(url, children, datasets);
                }
            }

        }
        return datasets;
    }



}
