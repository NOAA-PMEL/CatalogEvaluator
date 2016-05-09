package pmel.sdig.cleaner

import grails.transaction.Transactional
import net.sf.ehcache.search.aggregator.Count
import org.jdom2.Document
import org.jdom2.Element
import org.jdom2.Namespace
import org.jdom2.Parent
import org.jdom2.output.Format
import org.jdom2.output.XMLOutputter
import org.jdom2.util.IteratorIterable
import org.springframework.web.context.request.RequestContextHolder
import pmel.sdig.cleaner.util.Util

import java.security.MessageDigest
import java.util.regex.Pattern


class CleanService {

    static transactional = false

    Namespace ns = Namespace.getNamespace("http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0");
    Namespace netcdfns = Namespace.getNamespace("http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2");
    Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");

    String viewer_0 = "http://ferret.pmel.noaa.gov/geoideLAS/getUI.do?data_url=";
    String viewer_0_description = ", Visualize with Live Access Server";

    String viewer_1 = "http://upwell.pfeg.noaa.gov/erddap/search/index.html?searchFor=";
    String viewer_1_description = ", Visualize with ERDDAP";

    String viewer_2 = "http://www.ncdc.noaa.gov/oa/wct/wct-jnlp.php?singlefile=";
    String viewer_2_description = ", Weather and Climate Toolkit";

    String threddsServer = "http://thredds.gov";
    String threddsContext = "geoide";
    String threddsServerName = "waitWhat";

    def clean(String parent, String url) {

        Catalog catalog = Catalog.findByParentAndUrl(parent, url)
        if ( catalog ) {
            try {

                if ( catalog.status != Catalog.Status.DOWNLOAD_FAILED ) {
                    doClean(catalog)
                }


            } catch (Exception e) {
                log.error("Failed to clean catalog. " + e.getMessage(), e)
            }
        }
    }

    def doClean(Catalog catalog) {
        if (catalog.getSubCatalogs()) {
            catalog.getSubCatalogs().each {
                log.debug("Cleaning sub-catalog " + it.getUrl())
                clean(catalog.getUrl(), it.getUrl())
            }

        }
        // Start with the rubric
        log.debug("make the rubric")

        Rubric rubric = catalog.getRubric()
        int good = 0
        int bad = 0
        if (catalog.getLeafNodeReferences()) {
            catalog.getLeafNodeReferences().each {
                LeafDataset ld = LeafDataset.findByUrl(it.getUrl())
                if (ld && ld.getNetCDFVariables() && ld.getNetCDFVariables().size() > 0) {
                    good++
                } else {
                    bad++
                }
            }
        }
        if (rubric) {
            rubric.setLeaves(good)
            rubric.setBadLeaves(bad)
        } else {
            log.debug("Making new rubric for " + catalog.getUrl())
            rubric = new Rubric()
            rubric.setServices(0)
            rubric.setAggregated(0)
            rubric.setBadLeaves(0)
            rubric.setLeaves(good)
            rubric.setBadLeaves(bad)
            rubric.setShouldBeAggregated(0)
            rubric.setTotalServices(0)
            catalog.setRubric(rubric)
            rubric.setCatalog(catalog)

        }
        Catalog.lock(3)
        catalog.save(flush: true)

        log.debug("Cleaning " + catalog.url)

        String path = null;
        String leafurl = null
        String remoteBase = null;

        if (!catalog.leafNodeReferences || catalog.leafNodeReferences.size() == 0) {
            log.debug(" no leaf node in catalog " + catalog.getUrl())
        } else {
            LeafNodeReference lnr = catalog.leafNodeReferences.iterator().next();
            path = lnr.getUrlPath();
            leafurl = lnr.getUrl();
            remoteBase = leafurl.replace(path, "");
        }


        Map<String, List<LeafDataset>> aggregates = aggregate(catalog);

        CatalogXml catalogXml = CatalogXml.findByUrl(catalog.url)
        def parsedXml = new XmlParser().parseText(catalogXml.xml)

        def iter = parsedXml.depthFirst().iterator()
        log.debug("grabbed a iterator: " + iter.hasNext())
        Set<String> removed = new HashSet<String>();
        while (iter.hasNext()) {
            Node node = iter.next()

            def nodeName = null;
            if (node.name() instanceof groovy.xml.QName) {
                nodeName = node.name().getLocalPart()
            } else {
                nodeName = node.name()
            }
            log.debug("Found node:" + node.name())

            if (nodeName == "service") {
                log.debug("Found a service node")
                def type = node.attributes().get("serviceType").toString().toUpperCase(Locale.US);
                if (type != "COMPOUND") removed.add(type)
                node.parent().remove(node)
            }
            if (nodeName == "serviceName") {
                log.debug("Found a serviceName node")
                node.parent().remove(node)
            }

        }

        new XmlNodePrinter().print(parsedXml)

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter)

        new XmlNodePrinter(printWriter).print(parsedXml)

        String xmls = stringWriter.toString()
        Document doc = new Document()

        JDOMUtils.XML2JDOM(xmls.trim(), doc)

        int agg = 0;
        for (Iterator<String> agIt = aggregates.keySet().iterator(); agIt.hasNext();) {
            String key = agIt.next();
            List<LeafDataset> leaves = aggregates.get(key)
            if (leaves.size() == 1 && leaves.get(0).netCDFVariables?.size() > 0) {
                agg++;
            }

            log.debug("Data sets for this key " + key)
            for (int i = 0; i < leaves.size(); i++) {
                LeafDataset leafDataset = leaves.get(i);
                log.debug("\t Includes " + leafDataset.getUrl())
            }
            addNCML(doc, catalog.getUrl(), leaves);


        }


        // Additioanl services only needed if there are data links in the catalog
        if (catalog.leafNodeReferences && catalog.leafNodeReferences.size() > 0) {
            Element service = new Element("service", ns);
            service.setAttribute("name", threddsServerName + "_compound");
            service.setAttribute("serviceType", "compound");
            service.setAttribute("base", "");

            int services = 0;
            List<String> missing_services = new ArrayList<String>();
            if (removed.contains("WMS")) {
                services++;
                addFullService(service, remoteBase.replace("dodsC", "wms"), "WMS");
            } else {
                rubric.addToMissingServices("WMS");
                addService(service, "wms", "WMS");
            }
            if (removed.contains("WCS")) {
                services++; ;
                addFullService(service, remoteBase.replace("dodsC", "wcs"), "WCS");
            } else {
                rubric.addToMissingServices("WCS");
                addService(service, "wcs", "WCS");
            }
            if (removed.contains("NCML")) {
                services++; ;
                addFullService(service, remoteBase.replace("dodsC", "ncml"), "NCML");
            } else {
                rubric.addToMissingServices("NCML");
                addService(service, "ncml", "NCML");
            }
            if (removed.contains("ISO")) {
                services++;
                addFullService(service, remoteBase.replace("dodsC", "iso"), "ISO");
            } else {
                rubric.addToMissingServices("ISO");
                addService(service, "iso", "ISO");
            }
            if (removed.contains("UDDC")) {
                services++; ;
                addFullService(service, remoteBase.replace("dodsC", "uddc"), "UDDC");
            } else {
                rubric.addToMissingServices("UDDC");
                addService(service, "uddc", "UDDC");
            }
            if (removed.contains("OPENDAP")) {
                services++;
                addFullService(service, remoteBase, "OPENDAP");
            } else {
                rubric.addToMissingServices("OPENDAP");
                addService(service, "dodsC", "OPENDAP");
            }
            rubric.setServices(services);
            doc.getRootElement().addContent(0, service);
        }

        // Put this at the top of the document in index 0.

        String filename;
        if (catalog.getParent() == "none") {
            filename = "CleanCatalog.xml"
        } else {
            String base = getFileBase(catalog.getUrl());
            File ffile = new File(base);
            ffile.mkdirs();
            filename = ffile.getPath() + File.separator + getFileName(catalog.getUrl())
        }
        write(filename, doc)

        // TODO deal with sub-catalogs when this catalog contains a "best time series"


        rubric.setAggregated(agg)
        if (!rubric.validate()) {
            rubric.errors.each {
                log.debug(it.toString())
            }
        } else {
            rubric.save()
        }
        Catalog.lock(4)
        catalog.save(flush: true)
        log.debug("The rubric is set catalog saved.")
    }
    def Map<String, List<LeafDataset>> aggregate(Catalog catalog) {


        String parent = catalog.getParent()

        Map<String, List<LeafDataset>> datasetGroups = new HashMap<String, List<LeafDataset>>();
        catalog.leafNodeReferences.each {LeafNodeReference leafNodeReference ->
            LeafDataset dataset = LeafDataset.findByUrl(leafNodeReference.getUrl());

            if ( leafNodeReference.status == LeafNodeReference.Status.CRAWL_FINISHED ) {

                log.debug("Found finished lnr "+leafNodeReference.url)

                if (!dataset.getUrl().endsWith("fmrc.ncd") ) {
                    String key = getAggregationSignature(dataset, false);
                    log.debug("Found agg sig "+key)
                    List<LeafDataset> datasets  = datasetGroups.get(key);
                    if ( datasets == null ) {
                        datasets = new ArrayList<LeafDataset>();
                        datasetGroups.put(key, datasets);
                    }
                    datasets.add(dataset);
                } else {
                    // Count FMRC data as bad.
                    // TODO we want to catch this at makerubric or ingest time
                    dataset.error = "Data set " + leafNodeReference.getUrl() + " looks to be a 2D FMRC.  We aren't including those for now.\""

                }


            } else {
                // TODO set the empty message on a data set during scan...

//                        errors.addMessage("Data set "+ leafNodeReference.getUrl()+" was scanned, but no CF compliant variables were found.");


            }
        }



        log.debug("eliminate duplicates from dataset groups with size="+datasetGroups.size())
        // First to eliminate groups with duplicates, if all datasets have the same start time they can be regrouped in to single sets.
        List<String> sameTimeRegroupKeys = new ArrayList<String>();
        for ( Iterator groupsIt = datasetGroups.keySet().iterator(); groupsIt.hasNext(); ) {
            String key = (String) groupsIt.next();
            List<LeafDataset> l = (List<LeafDataset>) datasetGroups.get(key);
            Collections.sort(l, new TimeComparator());
            log.debug("Times are sorted")
            if ( haveOverlappingTimes(l) ) {
                log.debug("ovelaping times for "+key)
                sameTimeRegroupKeys.add(key);
            }
        }
        if ( sameTimeRegroupKeys.size() > 0 ) {
            for ( Iterator strIt = sameTimeRegroupKeys.iterator(); strIt.hasNext(); ) {
                String key = (String) strIt.next();
                List<LeafDataset> datasets = datasetGroups.get(key);
                for ( Iterator dsIt = datasets.iterator(); dsIt.hasNext(); ) {
                    LeafDataset leaf = (LeafDataset) dsIt.next();
                    List<LeafDataset> newgroup = new ArrayList<LeafDataset>();
                    newgroup.add(leaf);
                    datasetGroups.put(key+leaf.getUrl(), newgroup);
                }
            }
        }
        log.debug("remove size="+sameTimeRegroupKeys.size())
        for ( Iterator keyIt = sameTimeRegroupKeys.iterator(); keyIt.hasNext(); ) {
            String key = (String) keyIt.next();
            datasetGroups.remove(key);
        }

        // Second, if not all the start times are the same, use the file names.
        List<String> regroupKeys = new ArrayList<String>();
        // Get each group and sort it by start date.
        for ( Iterator groupsIt = datasetGroups.keySet().iterator(); groupsIt.hasNext(); ) {
            String key = (String) groupsIt.next();
            List<LeafDataset> l = (List<LeafDataset>) datasetGroups.get(key);
            Collections.sort(l, new TimeComparator());
            if ( containsDuplicates(l) ) {
                regroupKeys.add(key);
            }
        }
        Map<String, List<LeafDataset>> datasetsReGrouped = new HashMap<String,List<LeafDataset>>();

        if ( regroupKeys.size() > 0 ) {
            List<String> successRegrouping = new ArrayList<String>();
            for ( Iterator keyIt = regroupKeys.iterator(); keyIt.hasNext(); ) {
                String key = (String) keyIt.next();
                List<LeafDataset> datasets = datasetGroups.get(key);
                Set<String> fileKeys = findFileKeys(2, datasets);
                for ( Iterator dsIt = datasets.iterator(); dsIt.hasNext(); ) {
                    LeafDataset leafDataset = (LeafDataset) dsIt.next();
                    String computedKey = getAggregationSignature(leafDataset, false);  // Get rid of longname options since we're using file names?
                    String regroupKey = computedKey;
                    for ( Iterator iterator = fileKeys.iterator(); iterator.hasNext(); ) {
                        String fkey = (String) iterator.next();
                        if ( leafDataset.getUrl().contains(fkey) ) {
                            successRegrouping.add(regroupKey);
                            regroupKey = regroupKey+fkey    ;
                        }
                    }
                    if ( !regroupKey.equals(computedKey) ) {
                        // We found a way to regroup these, so where done.
                        List<LeafDataset> regroupDatasets = datasetsReGrouped.get(regroupKey);
                        if ( regroupDatasets == null ) {
                            regroupDatasets = new ArrayList<LeafDataset>();
                            datasetsReGrouped.put(regroupKey, regroupDatasets);
                        }
                        regroupDatasets.add(leafDataset);
                    }
                }
            }

            log.debug("removing the regrouped keys")
            for ( Iterator keyIt = successRegrouping.iterator(); keyIt.hasNext(); ) {
                String key = (String) keyIt.next();
                datasetGroups.remove(key);
                regroupKeys.remove(key);
            }
            for ( Iterator keyIt = datasetsReGrouped.keySet().iterator(); keyIt.hasNext(); ) {
                String regroupKey = (String) keyIt.next();
                datasetGroups.put(regroupKey, datasetsReGrouped.get(regroupKey));
            }

            for ( Iterator keyIt = regroupKeys.iterator(); keyIt.hasNext(); ) {
                String key = (String) keyIt.next();
                List<LeafDataset> datasets = datasetGroups.get(key);
                for ( Iterator dsIt = datasets.iterator(); dsIt.hasNext(); ) {
                    LeafDataset leafDataset = (LeafDataset) dsIt.next();
                    System.out.println("This key: "+key);
                    System.out.println("\t"+leafDataset.getUrl());
                }
                System.out.println("Still has duplicates.");
            }
        }

        // Fill the aggregates object and return it.
        return datasetGroups;
    }

    def addService ( Element compoundService, String base, String type) {
        Element service = new Element("service", ns);
        service.setAttribute("name", threddsServerName+"_"+base);
        String fullbase = threddsServer;
        if ( !fullbase.endsWith("/")) fullbase = fullbase + "/";
        fullbase = fullbase + threddsContext;
        if ( !fullbase.endsWith("/") ) fullbase = fullbase + "/";
        fullbase = fullbase + base;
        if ( !fullbase.endsWith("/") ) fullbase = fullbase + "/";
        service.setAttribute("base", fullbase);
        service.setAttribute("serviceType", type);
        compoundService.addContent(service);
    }
    def addFullService(Element compoundService, String base, String type) {
        Element service = new Element("service", ns);
        service.setAttribute("name", base+"_"+type);
        service.setAttribute("base", base);
        service.setAttribute("serviceType", type);
        compoundService.addContent(service);
    }

    def getFileBase(String url) throws MalformedURLException {
        URL catalogURL = new URL(url);
        return "CleanCatalogs"+File.separator+catalogURL.getHost()+catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/"));
    }
    def getFileName(String url) throws MalformedURLException {
        URL catalogURL = new URL(url);
        return catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/"));
    }
    def write(String filename, Document doc) {

        XMLOutputter xout = new XMLOutputter();
        Format format = Format.getPrettyFormat();
        format.setLineSeparator(System.getProperty("line.separator"));
        xout.setFormat(format);
        PrintStream fout = new PrintStream(filename);
        xout.output(doc, fout);
    }

    private static String getAggregationSignature(LeafDataset dataset, boolean longname) throws UnsupportedEncodingException {

        String startTime = dataset.getRepresentativeTime();
        if ( startTime == null ) {
            startTime = String.valueOf(Math.random());
        }
        String signature = "";

        def variables = dataset.getNetCDFVariables();

        if ( variables != null && variables.size() > 0 ) {

            NetCDFVariable one = variables?.find{ it.timeAxis }
            TimeAxis tone;
            if ( one ) {
                tone = one.getTimeAxis();
            }

            if ( tone != null ) {
                startTime = tone.getStart();
                signature = signature + startTime;
            } else {
                signature = String.valueOf(Math.random()); // There is no time axis so there can be no aggregation, randomize the signature.
            }
            for ( Iterator<NetCDFVariable> varIt = variables.iterator(); varIt.hasNext(); ) {
                NetCDFVariable dsvar = varIt.next();
                if ( longname ) {
                    signature = signature + dsvar.getLongName();
                }
                List<StringAttribute> attrs = dsvar.getStringAttributes();
                boolean latLon = false;
                // These are coordinate variables.  They probably should have been eliminated in the data crawl, but since they weren't we'll get rid of them here.
                for (Iterator attIt = attrs.iterator(); attIt.hasNext();) {
                    StringAttribute stringAttribute = (StringAttribute) attIt.next();
                    if ( stringAttribute.getName().toLowerCase().equals("standard_name")  && stringAttribute.getValues().get(0).toLowerCase().equals("latitude") ||
                            stringAttribute.getName().toLowerCase().equals("standard_name")  && stringAttribute.getValues().get(0).toLowerCase().equals("longitude") ) {
                        latLon = true;
                    }
                    if ( stringAttribute.getName().toLowerCase().equals("statistic") && dataset.getUrl().contains("esrl") ) {
                        signature = signature+stringAttribute.getName()+stringAttribute.getValues().get(0);
                    }
                }
                if ( !latLon ) {

                    // toString for each axis object gives a summary that makes up the signature.
                    signature = signature + dsvar.getName() + dsvar.getRank() + dsvar.getGeoAxisX().summary() + dsvar.getGeoAxisY().summary();
                    if ( dsvar.getTimeAxis() != null ) {
                        TimeAxis t = dsvar.getTimeAxis();
                        if ( t != null ) {
                            String u = t.getUnits();
                            if ( u != null && u.contains("0000-") ) {
                                // Climatology units, do not aggregate...
                                return MD5Encode(String.valueOf(Math.random()));
                            }
                        }
                        signature = signature + dsvar.getTimeAxis();
                    }
                    if ( dsvar.getVerticalAxis() != null ) {
                        signature = signature + dsvar.getVerticalAxis();
                    }

                }

            }
        } else {
            // I don't know what the heck we're going to do with this data set since there are no variables, but we sure as heck ain't going to aggregate it.
            signature = String.valueOf(Math.random());
        }
        return MD5Encode(signature);
    }

/**
 * Make an MD5 Hash from a particular string.
 * @param str - the string to be hashed.
 * @return md5 - the resulting hash
 * @throws UnsupportedEncodingException
 */
    public static String MD5Encode(String str) throws UnsupportedEncodingException {
        String returnVal = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte mdArr = md.digest(str.getBytes("UTF-16"));
            returnVal = toHexString(mdArr);
        } catch (Exception e) {
            returnVal = URLEncoder.encode(str,"UTF-8").replaceAll("\\*", "x");
        }
        return returnVal;
    }
/**
 * Convert a collection of bytes from an MD5 hash to a string.
 * @param bytes the bytes to be converted
 * @return the resulting string
 */
    protected static String toHexString(byte bytes) {
        char chars = new char[bytes.length * 2];

        for (int i = 0; i < bytes.length; ++i) {
            chars[2 * i] = HEXCODE[(bytes[i] & 0xF0) >>> 4];
            chars[2 * i + 1] = HEXCODE[bytes[i] & 0x0F];
        }
        return new String(chars);
    }

    protected static final char[] HEXCODE = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' ];

    private static boolean haveOverlappingTimes(List<LeafDataset> datasets) {
        int i = 0;
        double firstTimeMin = 9999999999.0;
        double firstTimeMax = -9999999999.0;
        double min = 9999999999.0;
        double max = -9999999999.0;
        for ( Iterator dsIt = datasets.iterator(); dsIt.hasNext(); ) {
            LeafDataset dataset = (LeafDataset) dsIt.next();

            List<NetCDFVariable> variables = dataset.getNetCDFVariables();
            if ( variables != null && variables.size() > 0) {
                NetCDFVariable var = variables.get(0);
                TimeAxis t = var.getTimeAxis();
                if ( t != null ) {
                    if ( i == 0 ) {
                        firstTimeMin = t.getMin();
                        firstTimeMax = t.getMax();

                    } else {
                        min = t.getMin();
                        max = t.getMax();

                        if ( min > firstTimeMax || max < firstTimeMin ) {
                            return false;
                        }
                    }
                }
            }
            firstTimeMax = Math.max(firstTimeMax, max);
            firstTimeMin = Math.min(firstTimeMin, min);
            i++;
        }
        return true;
    }
    private static boolean containsDuplicates(List<LeafDataset> datasets) {
        List<String> startTimes = new ArrayList<String>();
        for ( Iterator dsIt = datasets.iterator(); dsIt.hasNext(); ) {
            LeafDataset dataset = dsIt.next();
            List<NetCDFVariable> variables = dataset.getNetCDFVariables();
            if ( variables != null && variables.size() > 0) {
                NetCDFVariable var = variables.get(0);
                TimeAxis t = var.getTimeAxis();
                if ( t != null ) {
                    String startTime = t.getStart();
                    if ( startTimes.contains(startTime) ) {
                        return true;
                    } else {
                        startTimes.add(startTime);
                    }
                }

            }
        }
        return false;
    }
    private static Set<String> findFileKeys(int iterations, List<LeafDataset> datasets) {
        Set<String> fileKeys = new HashSet<String>();
        List<String> filenames = new ArrayList<String>();
        String startTime = datasets.get(0).getLeafDataset().getRepresentativeTime();
        filenames.add(datasets.get(0).getLeafDataset().getUrl());
        for (int i = 1; i < datasets.size(); i++ ) {
            LeafDataset data = datasets.get(i).getLeafDataset();

            if ( data.getRepresentativeTime().equals(startTime) ) {
                filenames.add(data.getUrl());
            }
        }
        String base = filenames.get(0);
        for ( int i = 1; i < filenames.size(); i++ ) {
            fileKeys.addAll(Util.uniqueParts(iterations, base, filenames.get(i)));
        }

        return fileKeys;
    }

    private void addNCML(Document doc, String parent, List<LeafDataset> aggs) throws MalformedURLException {




        //TODO Get this from a configured domain object.



        boolean aggregating = aggs.size() > 1;

        def matchingDataset = null;

        LeafDataset dataOne = aggs.get(0);

        IteratorIterable datasetIt = doc.getRootElement().getDescendants(new UrlPathFilter(dataOne.getUrlPath()));
        int index = 0;
        Parent p = null;
        while (datasetIt.hasNext() ) {
            if ( index == 0 ) {
                Element dataset = (Element) datasetIt.next();
                matchingDataset = dataset;
                p = matchingDataset.getParent();
            }
            index++;
        }
        Element ncml = new Element("netcdf", netcdfns);

        Element geospatialCoverage = new Element("geospatialCoverage", ns);

        List<Element> properties = new ArrayList<Element>();

        Element variables = new Element("variables", ns);
        variables.setAttribute("vocabulary", "netCDF_contents");

        URL aggURL = new URL(dataOne.getUrl());


        Element aggregation = new Element("aggregation", netcdfns);
        if ( !aggregating ) {
            ncml.setAttribute("location", dataOne.getUrl());
            log.debug("Adding single data set location " + dataOne.getUrl())
        } else {
            aggregation.setAttribute("type", "joinExisting");
            ncml.addContent(aggregation);
            Element documentation = new Element("documentation", ns);
            documentation.setAttribute("type", "Notes");
            String catalogHTML = parent.substring(0, parent.lastIndexOf('.'))+".html";
            documentation.setAttribute("href", catalogHTML, xlink);
            documentation.setAttribute("title", "Aggregated from catalog "+catalogHTML+" starting with "+leafNode.getUrl().substring(0, leafNode.getUrl().lastIndexOf('/')), xlink);
            properties.add(documentation);
        }

        boolean rescan = false;


        if ( dataOne != null && dataOne.getNetCDFVariables().size() > 0) {


            /*
             * We can only promote LAS metadata for a data set that has one z-axis and one t-axis.
             * If it has more than one of each of these (ROMS model output for example) then we must
             * scan the original data set using addXML at the time this catalog is configure.
             *
             * We are now marking the data set for scanning with a property...
             */
            List<NetCDFVariable> repVariables = dataOne.getNetCDFVariables();
            Set<String> znames = new HashSet<String>();
            Set<String> tnames = new HashSet<String>();
            for (Iterator repVarIt = repVariables.iterator(); repVarIt.hasNext();) {
                NetCDFVariable netCDFVariable = (NetCDFVariable) repVarIt.next();
                TimeAxis repTimeAxis = netCDFVariable.getTimeAxis();
                VerticalAxis repZAxis = netCDFVariable.getVerticalAxis();
                if ( repTimeAxis != null ) {
                    tnames.add(repTimeAxis.getName());
                }
                if ( repZAxis != null ) {
                    znames.add(repZAxis.getName());
                }
            }

            // If there is more than one group, then this will be a loop and we will be putting the variables into data sets and
            // using the matching data set as a container.  If not, the code will look as it does now.
            log.debug("There are "+tnames.size()+" time axes and "+znames.size()+" z axes.");

            if ( tnames.size() > 1 || znames.size() > 1 ) {
                rescan = true;
            }
            // Always do the XY metadata...
            NetCDFVariable representativeVariable = dataOne.getNetCDFVariables().get(0);

            log.debug("Setting XY metadata")

            GeoAxisY yaxis = representativeVariable.getGeoAxisY();
            double latmax = representativeVariable.getLatMax();
            double latmin = representativeVariable.getLatMin();
            double latsize = latmax - latmin;
            String latunits = yaxis.getUnits();
            if ( latunits == null ) {
                latunits = "degN";
            }
            Element northsouth = new Element("northsouth", ns);
            Element ystart = new Element("start", ns);
            Element ysize = new Element("size", ns);
            Element yunits = new Element("units", ns);
            ystart.setText(String.valueOf(latmin));
            ysize.setText(String.valueOf(latsize));
            yunits.setText(latunits);
            northsouth.addContent(ystart);
            northsouth.addContent(ysize);
            northsouth.addContent(yunits);
            geospatialCoverage.addContent(northsouth);

            GeoAxisX xaxis = representativeVariable.getGeoAxisX();
            double lonmax = representativeVariable.getLonMax();
            double lonmin = representativeVariable.getLonMin();
            double lonsize = lonmax - lonmin;
            String lonunits = xaxis.getUnits();
            if ( lonunits == null ) {
                lonunits = "degE";
            }
            Element eastwest = new Element("eastwest", ns);
            Element xstart = new Element("start", ns);
            Element xsize = new Element("size", ns);
            Element xunits = new Element("units", ns);
            xstart.setText(String.valueOf(lonmin));
            xsize.setText(String.valueOf(lonsize));
            xunits.setText(lonunits);
            eastwest.addContent(xstart);
            eastwest.addContent(xsize);
            eastwest.addContent(xunits);
            geospatialCoverage.addContent(eastwest);

            Element ewPropertyNumberOfPoints = new Element("property", ns);
            ewPropertyNumberOfPoints.setAttribute("name", "eastwestNumberOfPoints");
            ewPropertyNumberOfPoints.setAttribute("value", String.valueOf(xaxis.getSize()));

            properties.add(ewPropertyNumberOfPoints);

            Element ewPropertyResolution = new Element("property", ns);
            ewPropertyResolution.setAttribute("name", "eastwestResolution");
            ewPropertyResolution.setAttribute("value", String.valueOf(lonsize/Double.valueOf(xaxis.getSize()-1)));

            properties.add(ewPropertyResolution);

            Element ewPropertyStart = new Element("property", ns);
            ewPropertyStart.setAttribute("name", "eastwestStart");
            ewPropertyStart.setAttribute("value", String.valueOf(lonmin));

            properties.add(ewPropertyStart);
            Element nsPropertyNumberOfPoints = new Element("property", ns);
            nsPropertyNumberOfPoints.setAttribute("name", "northsouthNumberOfPoints");
            nsPropertyNumberOfPoints.setAttribute("value", String.valueOf(yaxis.getSize()));

            properties.add(nsPropertyNumberOfPoints);

            Element nsPropertyResolution = new Element("property", ns);
            nsPropertyResolution.setAttribute("name", "northsouthResolution");
            nsPropertyResolution.setAttribute("value", String.valueOf(latsize/Double.valueOf(yaxis.getSize()-1)));

            properties.add(nsPropertyResolution);

            Element nsPropertyStart = new Element("property", ns);
            nsPropertyStart.setAttribute("name", "northsouthStart");
            nsPropertyStart.setAttribute("value", String.valueOf(latmin));

            properties.add(nsPropertyStart);
            if ( rescan ) {

                Element rescanProperty = new Element("property", ns);
                rescanProperty.setAttribute("name", "LAS_scan");
                rescanProperty.setAttribute("value", "true");
                properties.add(rescanProperty);

            } else {


                // Only do the ZT metadata if there is at most one of each such axis.

                log.debug("Setting ZT metadata if applicable.")

                String hasZ = "";
                String hasT = "";

                for ( Iterator rVarIt = dataOne.getNetCDFVariables().iterator(); rVarIt.hasNext(); ) {
                    NetCDFVariable rVar = (NetCDFVariable) rVarIt.next();
                    VerticalAxis vert = rVar.getVerticalAxis();
                    if ( vert != null ) {
                        if ( hasZ.equals("") ) {
                            log.debug("Has z...")
                            String positive = vert.getPositive();
                            if ( positive != null ) {
                                geospatialCoverage.setAttribute("zpositive", positive);
                            }
                            String min = String.valueOf(vert.getMin());
                            double size = vert.getMax() - vert.getMin();
                            String units = vert.getUnits();
                            if ( units == null ) {
                                units = "";
                            }
                            Element updown = new Element("updown", ns);
                            Element zStart = new Element("start", ns);
                            Element zSize = new Element("size",ns);
                            Element zUnits = new Element("units", ns);
                            Element zResolution = new Element("resolution", ns);
                            zStart.setText(min);
                            updown.addContent(zStart);
                            zSize.setText(String.valueOf(size));
                            updown.addContent(zSize);
                            zUnits.setText(units);
                            updown.addContent(zUnits);
                            if ( !Double.isNaN( vert.getDelta()) ) {
                                zResolution.setText(String.valueOf(vert.getDelta()));
                                updown.addContent(zResolution);
                            }
                            geospatialCoverage.addContent(updown);

                            List<Zvalue> zees = vert.getZvalues();

                            if ( zees != null ) {
                                Element property = new Element("property", ns);
                                property.setAttribute("name", "updownValues");
                                String values = "";


                                for ( int i = 0; i < zees.size(); i++ ) {
                                    values = values + String.valueOf(zees.get(i).getValue()) + " ";
                                }
                                property.setAttribute("value", values.trim());
                                properties.add(property);
                            } else {
                                Element property = new Element("property", ns);
                                property.setAttribute("name", "updownNumberOfPoints");
                                property.setAttribute("value", String.valueOf(vert.getSize()));
                                properties.add(property);
                            }
                        }

                        hasZ = hasZ + rVar.getName() + " ";


                    }
                    TimeAxis taxis = rVar.getTimeAxis();


                    if ( taxis != null ) {
                        hasT = hasT + rVar.getName() + " ";
                    }
                }
                if ( !hasZ.equals("") ) {
                    Element hasZProperty = new Element("property", ns);
                    hasZProperty.setAttribute("name", "hasZ");
                    hasZProperty.setAttribute("value", hasZ.trim());
                    properties.add(hasZProperty);
                }
                if ( !hasT.equals("") ) {
                    Element hasTProperty = new Element("property", ns);
                    hasTProperty.setAttribute("name", "hasT");
                    hasTProperty.setAttribute("value", hasT.trim());
                    properties.add(hasTProperty);
                }
            }

        } else {
            if ( dataOne == null ) {
                System.err.println("Data node information is null for "+parent+" ... "+dataOne.getUrl());
            }
            if ( dataOne.getNetCDFVariables().size() == 0 ) {
                System.err.println("Data node has no data variables "+parent+" ... "+dataOne.getUrl());
            }
            return;
        }
        String timeStart = "";
        String timeEnd = "";
        long timeSize = 0;
        boolean time_units_done = false;
        for ( int a = 0; a < aggs.size(); a++ ) {
            LeafDataset dataset = aggs.get(a);
            Element netcdf = new Element("netcdf", netcdfns);
            netcdf.setAttribute("location", dataset.getUrl());
            if ( dataset.getNetCDFVariables() != null && dataset.getNetCDFVariables().size() > 0 ) {
                List<NetCDFVariable> vars = dataset.getNetCDFVariables();
                boolean done = false;

                // Find the first variable that has a time axis and use it as the basis for the time data for this dataset.
                if ( !rescan ) {
                    log.debug("Setting t metadata...")
                    for ( Iterator iterator = vars.iterator(); iterator.hasNext(); ) {
                        NetCDFVariable netCDFVariable = (NetCDFVariable) iterator.next();

                        TimeAxis ta = netCDFVariable.getTimeAxis();
                        if ( ta != null && !done) {

                            if ( !time_units_done ) {
                                Element timeUnitsProperty = new Element("property", ns);
                                timeUnitsProperty.setAttribute("name", "timeAxisUnits");
                                timeUnitsProperty.setAttribute("value", ta.getUnits());
                                properties.add(timeUnitsProperty);

                                time_units_done = true;
                            }

                            done = true;
                            aggregation.setAttribute("dimName", ta.getName());

                            if ( ta != null ) {
                                if ( a == 0 ) {
                                    timeStart = ta.getStart();
                                }
                                if ( a == aggs.size() - 1 ) {
                                    timeEnd = ta.getEnd();
                                }
                                long tsize = ta.getSize();
                                timeSize = timeSize + tsize;

                                netcdf.setAttribute("ncoords", String.valueOf(tsize));
                            }
                            aggregation.addContent(netcdf);
                        }
                    }
                }
            }
        }
        if ( !rescan ) {
            Element timeCoverageStart = new Element("property", ns);
            timeCoverageStart.setAttribute("name", "timeCoverageStart");
            timeCoverageStart.setAttribute("value", timeStart);
            properties.add(timeCoverageStart);

            Element timeSizeProperty = new Element("property", ns);
            timeSizeProperty.setAttribute("name", "timeCoverageNumberOfPoints");
            timeSizeProperty.setAttribute("value", String.valueOf(timeSize));
            properties.add(timeSizeProperty);


            Element time = new Element("timeCoverage", ns);
            Element start = new Element("start", ns);
            start.setText(timeStart);
            Element end = new Element("end", ns);
            end.setText(timeEnd);
            time.addContent(start);
            time.addContent(end);

            properties.add(time);
        }
        String name = "";
        log.debug("Setting variable list metadata...")
        for ( Iterator varIt = dataOne.getNetCDFVariables().iterator(); varIt.hasNext(); ) {
            NetCDFVariable var = (NetCDFVariable) varIt.next();

            //<variable name="vwnd" units="m/s" vocabulary_name="mean Daily V wind" />
            Element variable = new Element("variable", ns);
            String vname = var.getName();

            name = name + vname;
            if ( varIt.hasNext() ) name = name + "_";

            if ( vname != null   ) {
                variable.setAttribute("name", vname);
            }
            String units = var.getUnitsString();
            if ( units != null ) {
                variable.setAttribute("units", units);
            }
            String longname = var.getLongName();
            if ( longname != null ) {
                variable.setAttribute("vocabulary_name", longname);
            } else {
                variable.setAttribute("vocabulary_name", vname);
            }

            variables.addContent(variable);

        }

        String dataURL;

        if ( matchingDataset != null && aggregating) {
            String path = aggURL.getPath();
            path = path.substring(path.lastIndexOf("dodsC/")+6, path.lastIndexOf('/')+1);
            matchingDataset.setAttribute("urlPath", path+name+"_aggregation");
            matchingDataset.setAttribute("name", name);
            dataURL = threddsServer;
            if ( !dataURL.endsWith("/")) dataURL = dataURL + "/";
            dataURL = dataURL+threddsContext+"/dodsC/"+path+name+"_aggregation";

        } else {
            dataURL = dataOne.getUrl();
        }

        if ( p != null ) {
            for ( int i = 1; i < aggs.size(); i++ ) {
                LeafDataset l = aggs.get(i);
                p.removeContent(new UrlPathFilter(l.getUrlPath()));
            }
        }

        String id;
        if ( matchingDataset != null ) {

            log.debug("Setting viewers...")

            Element service = new Element("serviceName", ns);
            service.addContent( threddsServerName+"_compound");

            Element mymetadata = new Element("metadata", ns);
            mymetadata.setAttribute("inherited", "true");
            mymetadata.addContent(variables);
            matchingDataset.addContent(0, mymetadata);
            matchingDataset.addContent(ncml);
            String tid = matchingDataset.getAttributeValue("ID");
            id = fixid(tid, dataURL);
            matchingDataset.setAttribute("ID", id);


            Element viewer0Property = new Element("property", ns);
            viewer0Property.setAttribute("name", "viewer_0");
            viewer0Property.setAttribute("value", viewer_0+dataURL+viewer_0_description);

            properties.add(viewer0Property);

            Element viewer1Property = new Element("property", ns);
            viewer1Property.setAttribute("name", "viewer_1");
            viewer1Property.setAttribute("value", viewer_1+dataURL+viewer_1_description);

            properties.add(viewer1Property);

            Element viewer2Property = new Element("property", ns);
            viewer2Property.setAttribute("name", "viewer_2");
            viewer2Property.setAttribute("value", viewer_2+dataURL+viewer_2_description);

            properties.add(viewer2Property);
            matchingDataset.addContent(0, service);
            matchingDataset.addContent(0, geospatialCoverage);
            matchingDataset.addContent(0, properties);


        }
    }

    public String fixid(String tid, String name) {
        String id;
        if ( tid != null ) {
            id = tid;
            id = id.replace("/", ".");
            id = id.replace(":", ".");
            id = id.replace(",","");
            if ( Pattern.matches("^[0-9].*", id) ) id = id + "dataset-";
            id = id.replaceAll(" ", "-");
        } else {
            // This should never happen in the UAF case since the cleaner should have assigned the ID already.
            try {
                id = "data-"+JDOMUtils.MD5Encode(name)+String.valueOf(Math.random());
            } catch (UnsupportedEncodingException e) {
                id = "data-"+String.valueOf(Math.random());
            }
        }
        return id;
    }
}
