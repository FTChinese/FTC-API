package com.ftchinese.workers

import java.util.{Calendar, TimeZone}

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Number 10003 seeder
 * Created by wanbo on 2015/4/16.
 */
final class Seeder_10003 extends Seeder with ISeeder {

    name = "10003"

    driver = new MysqlDriver

    private var _days: Int = 0
    private var _topNum: Int = 10

    private val log = LoggerFactory.getLogger(classOf[Seeder_10003])

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
                        obj = obj + ("storyid" -> x._1)
                        obj = obj + ("cheadline" -> x._2)
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
    override protected def onDBHandle(): List[(String, String)] = {
        var dataList = List[(String, String)]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            val calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"))
            calendar.add(Calendar.DATE, _days * -1)

            val sql = "SELECT c.storyid,s.cheadline,count(storyid) views FROM `comment` c, `story` s where c.`ischecked`=1 and c.dnewdate > from_unixtime(%d) and s.publish_status='publish' and c.storyid=s.id group by c.storyid order by views desc limit %d".format(calendar.getTimeInMillis / 1000, _topNum)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                dataList = dataList :+ (rs.getString(1), rs.getString(2))
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