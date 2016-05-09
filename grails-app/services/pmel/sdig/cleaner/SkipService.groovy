package pmel.sdig.cleaner

import grails.transaction.Transactional
import groovy.util.slurpersupport.NodeChild

import java.util.regex.Pattern
import java.util.stream.Collectors

@Transactional
class SkipService {

    TreeService treeService

    def configure(String url) {
        Skip skip = new Skip();

        if ( url ) {
            InputStream lexicon = this.class.classLoader.getResourceAsStream(url)
            if ( lexicon ) {
                String sourceXml = new BufferedReader(new InputStreamReader(lexicon)).lines().collect(Collectors.joining("\n"));

                def host = null
                def port = 80
                def curls = treeService.findCatalogRefNodes(host, port, url, sourceXml)


                if (curls) {
                    curls.each { String refurl ->
                        skip.addToSkipCatalogs(refurl)
                    }
                }
                def regex = findRegExProperties(sourceXml)
                if (regex) {
                    regex.each { String regx ->
                        skip.addToSkipRegExes(regx)
                    }
                }
                skip.save();
            }
        }


    }
    def boolean skip(String parent, String url) {

        Skip skip = Skip.first()

        if (skip) {

            if (skip.skipCatalogs && skip.skipCatalogs.contains(url)) {
                return true
            }

            if ( skip.skipRegExes ) {
                skip.skipRegExes.each { String reg ->

                    if (Pattern.matches(reg, url)) {
                        return true;
                    }

                }
            }
        }

        return false

    }
    def List<String> findRegExProperties (String xml) {

        List<String> regexs = new ArrayList<String>();
        def childParsedXml = new XmlSlurper().parseText(xml)

        def refs = childParsedXml.'**'.findAll { node -> node.name() == "property" }

        refs.each { NodeChild node ->

            def attrs = node.attributes()

            def name = attrs.get('name')
            if (name.equals("regex")) {
                def value = attrs.get("value")
                regexs.add(value)

            }
        }
        return regexs
    }
}
