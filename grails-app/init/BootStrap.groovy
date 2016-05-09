import pmel.sdig.cleaner.SkipService

class BootStrap {

    SkipService skipService

    def init = { servletContext ->

        skipService.configure("skip.xml")

    }
    def destroy = {
    }
}
