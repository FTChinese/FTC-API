package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get the latest stories.
 * Created by wanbo on 2015/11/6.
 */
final class Seeder_10001 extends Seeder with ISeeder {

    name = "10001"

    driver = new MysqlDriver

    private var _topNum: Int = 10
    private val _maxNum: Int = 100

    private var _withPic: Boolean = false

    private val log = LoggerFactory.getLogger(classOf[Seeder_10001])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {

            val startRunTime = System.currentTimeMillis()

            val num = seed.getOrElse("num", "10").toString
            if (num == null || !num.forall(_.isDigit)){
                _topNum = 10
            } else {
                _topNum = num.toInt
            }

            if(_topNum > _maxNum)
                _topNum = _maxNum

            val withPicStr: String = seed.getOrElse("withpic", "0").toString

            if(withPicStr == "1"){
                _withPic = true
            }

            // Cache
            val cache_name = this.getClass.getSimpleName + _topNum + _withPic

            val cacher = new CacheManager(conf = _conf, expire = 600)

            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                dataList = onDBHandle()

                if (dataList.size < 1)
                    throw new EasyException("20100")
                else {
                    val cache_data = new EasyOutput

                    if (_withPic) {
                        val sIds = dataList.map(_.getOrElse("id", "").toString).filter(_ != "").mkString(",")
                        val picData = manager.transform("10006", Map(("storyid", sIds)))

                        if (picData.oelement.get("errorcode").get == "0") {
                            dataList = dataList.map(x => {
                                val tmpId = x.getOrElse("id", "")
                                var picMap = Map[String, String]()
                                picData.odata.foreach(y => {
                                    if (y.getOrElse("ostoryid", "") == tmpId) {
                                        val otype = y.getOrElse("otype", "").toString.toLowerCase
                                        if (otype == "other")
                                            picMap += "smallbutton" -> y.getOrElse("olink", "").toString
                                        else
                                            picMap += otype -> y.getOrElse("olink", "").toString
                                    }
                                })
                                x updated("story_pic", picMap)
                            })
                        }
                    }

                    cache_data.odata = dataList

                    cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
                    cacher.cacheData(cache_name, cache_data)
                }
            }
            cacher.close()

            fruits.oelement = fruits.oelement.updated("errorcode", "0").+("duration" -> (System.currentTimeMillis() - startRunTime).toString)
            fruits.odata = dataList
        } catch {
            case ee: EasyException =>
                fruits.oelement = fruits.oelement.updated("errorcode", ee.getCode)
            case e: Exception =>
                log.error("Seeder has exception:", e)
                fruits.oelement = fruits.oelement.updated("errorcode", "-1")
                fruits.oelement = fruits.oelement.updated("errormsg", e.getMessage)
        }

        fruits
    }
    override protected def onDBHandle(): List[Map[String, String]] = {
        var dataList = List[Map[String, String]]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            val sql = "select id,cheadline,cskylineheadline,cskylinetext,clongleadbody,cshortleadbody,cbyline_description,cauthor,eheadline, eskylineheadline, eskylinetext, elongleadbody, eshortleadbody, ebyline_description, eauthor,tag,genre,topic,`column`,priority,pubdate,from_unixtime(pubdate,'%Y%m%d') orderdate  from story where `publish_status` = 'publish' order by orderdate desc,`priority`,`fileupdatetime` desc limit " + _topNum

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            val metaData = ps.getMetaData
            val columnCount = metaData.getColumnCount
            while (rs.next()){
                var tmpMap = Map[String, String]()

                for(i <- Range(1, columnCount + 1)) {
                    tmpMap = tmpMap + (metaData.getColumnLabel(i) -> rs.getString(i))
                }

                dataList = dataList :+ tmpMap
            }

            rs.close()
            ps.close()
            conn.close()

        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }

}