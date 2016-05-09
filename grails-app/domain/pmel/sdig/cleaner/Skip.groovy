package pmel.sdig.cleaner

import java.util.regex.Pattern

class Skip {

    List skipCatalogs
    List skipRegExes
    static hasMany = [skipCatalogs: String, skipRegExes: String]

    static constraints = {
        skipCatalogs nullable: true
        skipRegExes nullable: true
    }



}
