package com.ftchinese.utils

/**
 * Utility functions.
 * Created by wanbo on 2015/11/12.
 */
object Utils {

    def formatRealImgUrl(imgPath: String): String ={
        if(imgPath.startsWith("/upload"))
            imgPath.replace("/upload", "http://i.ftimg.net")
        else
            imgPath
    }
}
