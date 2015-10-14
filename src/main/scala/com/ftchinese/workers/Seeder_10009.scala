package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get the related video by story id.
 * Created by wanbo on 2015/10/10.
 */
final class Seeder_10009 extends Seeder with ISeeder {

    name = "10009"

    driver = new MysqlDriver

    private var _storyId = ""

    private val log = LoggerFactory.getLogger(classOf[Seeder_10009])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val storyId = seed.getOrElse("storyid", "").toString

            if (storyId != null && !storyId.forall(_.isDigit))
                throw new EasyException("20001")

            if(storyId != null)
                _storyId = storyId

            // Cache
            val cache_name = this.getClass.getSimpleName + _storyId

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
                    cache_data.odata = dataList

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
    override protected def onDBHandle(): List[Map[String, String]] = {
        var dataList = List[Map[String, String]]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            val sqlList = "SELECT `relate_id` FROM `story2story` where `st_id` = %s and length(`relate_id`) < 9".format(_storyId)

            var ps = conn.prepareStatement(sqlList)
            var rs = ps.executeQuery()

            var idList = Set[String]()
            while (rs.next()) {
                idList += rs.getString(1)
            }

            if(idList.size > 0) {

                val sql = "SELECT id,cheadline FROM `video_story` where id in (%s)".format(idList.mkString("'", "','", "'"))

                ps = conn.prepareStatement(sql)
                rs = ps.executeQuery()

                while (rs.next()) {
                    var tmpMap = Map[String, String]()
                    tmpMap = tmpMap + ("id" -> rs.getString(1))
                    tmpMap = tmpMap + ("cheadline" -> rs.getString(2))

                    dataList = dataList :+ tmpMap
                }
            } else {
                rs.close()
                ps.close()
                throw new EasyException("20100")
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