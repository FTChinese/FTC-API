package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get the hot news by days.
 * Created by wanbo on 2015/10/14.
 */
final class Seeder_60006 extends Seeder with ISeeder {

    name = "60006"

    driver = new MysqlDriver

    private var _days: Int = 0
    private var _topNum: Int = 10

    private val log = LoggerFactory.getLogger(classOf[Seeder_60006])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val days: String = seed.getOrElse("days", "1").toString

            if (days != null && !days.forall(_.isDigit))
                throw new EasyException("20001")

            _days = days.toInt

            if(_days > 100)
                _days = 100

            val num = seed.getOrElse("num", "10").toString
            if (num == null || !num.forall(_.isDigit)){
                _topNum = 10
            } else {
                _topNum = num.toInt
            }

            if(_topNum > 30)
                _topNum = 30

            // Cache
            val cache_name = this.getClass.getSimpleName + _days + _topNum

            val cacher = new CacheManager(conf = _conf, expire = 10800)

            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                val data = onDBHandle()

                if (data.size < 1)
                    throw new EasyException("20100")
                else {

                    val idList = data.map(x => x.get("storyid").get).mkString(",")

                    val storyData = manager.transform("10002", Map(("storyid", idList)))

                    if(storyData.oelement.get("errorcode").get == "0"){
                        val data1 = data.map(x => (x.get("storyid").get, x.get("totalpv").get))
                        val data2 = storyData.odata.map(x => (x.get("id").get, x.get("cheadline").get))

                        val mergeData = data1 ++ data2
                        dataList = mergeData.groupBy(_._1).map(x => x._1 -> x._2.map(_._2)).map(y => Map("storyid" -> y._1, "totalpv" -> y._2.head, "title" -> y._2(1))).toList

                        val cache_data = new EasyOutput
                        cache_data.odata = dataList.sortBy(x => x.getOrElse("totalpv", 0).toString.toInt)(Ordering.Int.reverse)
                        cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
                        cacher.cacheData(cache_name, cache_data)
                    } else {
                        throw new EasyException(storyData.oelement.get("errorcode").get)
                    }

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
            val conn = driver.getConnector("analytic")

            val _type = _days match {
                case 7 =>
                    "week"
                case 30 =>
                    "month"
                case _ =>
                    "day"
            }

            val sql = "SELECT " + _type + " storyid, " + _type + "_num totalpv FROM most_popular"

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()) {
                var tmpMap = Map[String, String]()
                tmpMap = tmpMap + ("storyid" -> rs.getString(1))
                tmpMap = tmpMap + ("totalpv" -> rs.getString(2))
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