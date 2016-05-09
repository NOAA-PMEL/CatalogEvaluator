package pmel.sdig.cleaner

class RubricController {

    static scaffold = Rubric

    def chart() {
        def url = params.url
        if (url) {

            def parent = params.parent
            if (!parent) parent = "none"

            List<Rubric> children = new ArrayList<Rubric>()
            Catalog catalog = Catalog.findByParentAndUrl(parent, url)
            Rubric rubric = catalog.getRubric()
            catalog.subCatalogs.each {Catalog child ->
                Rubric childrubric = child.getRubric();
                children.add(childrubric)
            }
            render view: "chart", model: [rubric: rubric, children: children]
        }
    }


}
