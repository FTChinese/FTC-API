package com.ftchinese.utils

/**
  * Global Content Types
  *
  * Created by wanbo on 16/5/26.
  */
object ContentTypes {

    val STORY = 1
    val VIDEO = 2
    val INTERACTIVE = 3
    val PHOTONEWS = 4

    def name(t: Int): String = {
        t match {
            case STORY => "story"
            case VIDEO => "video"
            case INTERACTIVE => "interactive"
            case PHOTONEWS => "photonews"
            case _ => ""
        }
    }
}
