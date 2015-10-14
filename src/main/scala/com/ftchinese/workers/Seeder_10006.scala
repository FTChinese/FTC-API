package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get the picture of story.
 *
 * Created by wanbo on 2015/6/29.
 */
final class Seeder_10006 extends Seeder with ISeeder {

    name = "10006"

    driver = new MysqlDriver

    private var _storyIds: String = ""

    private val log = LoggerFactory.getLogger(classOf[Seeder_10006])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val storyIds: String = seed.getOrElse("storyid", "").toString

            if (storyIds.length < 1 || storyIds.length > 500)
                throw new EasyException("20001")

            val sList = storyIds.split(",")

            _storyIds = sList.filter(x => x.length == 9 && x.forall(_.isDigit)).mkString("','")

            if(_storyIds.length < 1)
                throw new EasyException("20001")

            // Cache
            val cache_name = this.getClass.getSimpleName + _storyIds

            val cacher = new CacheManager(conf = _conf, expire = 600)

            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                val data = onDBHandle()

                if (data.size < 1)
                    throw new EasyException("20100")
                else {
                    val cache_data = new EasyOutput
                    cache_data.odata = List[Map[String, Any]]()
                    data.foreach(x => {
                        var obj = Map[String, Any]()
                        obj = obj + ("ostoryid" -> x._1)
                        obj = obj + ("otype" -> x._2)
                        obj = obj + ("olink" -> x._3)
                        dataList = dataList :+ obj

                        cache_data.odata = cache_data.odata :+ obj
                    })
                    cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
                    cacher.cacheData(cache_name, cache_data)
                }
            }
            cacher.close()

            fruits.oelement = fruits.oelement.updated("errorcode", "0")
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
    override protected def onDBHandle(): List[(String, String, String)] = {
        var dataList = List[(String, String, String)]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            val sql = "select a.storyid,a.pictype,b.piclink from `story_pic` a left join `picture` b on a.`picture_id` = b.`id` where a.storyid in ('%s');".format(_storyIds)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                dataList = dataList :+ (rs.getString(1), rs.getString(2), rs.getString(3))
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