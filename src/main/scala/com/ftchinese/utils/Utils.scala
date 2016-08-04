package com.ftchinese.utils

/**
 * Utility functions.
 * Created by wanbo on 2015/11/12.
 */
object Utils {

    /**
      * Format the real image link.
      * @param imgPath      The image link read from database.
      * @return             The real link.
      */
    def formatRealImgUrl(imgPath: String): String ={
        if(imgPath.startsWith("/upload"))
            imgPath.replace("/upload", "http://i.ftimg.net")
        else
            imgPath
    }

    /**
      * Convert the integer StoryId to special length string.
      * @param storyId      The integer format StoryId.
      * @return             The special format string.
      */
    def formatStoryId(storyId: String): String ={
        if(storyId.isInstanceOf[String]){
            storyId.reverse.padTo(9, 0).reverse.mkString
        } else ""
    }

    /**
      * Reallocate a List by specific rule.
      * @param dataList     The List need to reallocate.
      * @param regular      The specific rules.
      * @tparam T           The type of each item in the List.
      * @return             The reallocated List.
      */
    def dataBalance[T](dataList: List[List[T]], regular: List[Int]): List[T] ={
        val total = regular.sum
        var retList = List[T]()

        var currentListSize = 0

        var index = 0
        regular.foreach(x => {
            var subSize = total - retList.size

            if(subSize < x)
                subSize = x

            val tmpList = dataList(index).slice(0, subSize)

            if(tmpList.size >= x) {
                if(currentListSize > 0)
                    retList = retList.slice(0, currentListSize) ++ tmpList
                else
                    retList = tmpList
            } else {
                retList = retList.slice(0, total - tmpList.size) ++ tmpList
            }

            currentListSize += x
            index += 1
        })

        retList
    }
}
