package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get the hot tags.
 * Created by wanbo on 2016/05/26.
 */
final class Seeder_60020 extends Seeder with ISeeder {

    name = "60020"

    driver = new MysqlDriver

    private val _type: String = "myft"
    private var _topNum: Int = 10

    private val log = LoggerFactory.getLogger(classOf[Seeder_60020])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val num = seed.getOrElse("topnum", "10").toString
            if (num == null || !num.forall(_.isDigit)){
                _topNum = 10
            } else {
                _topNum = num.toInt
            }

            if(_topNum > 30)
                _topNum = 30

            // Cache
            val cache_name = this.getClass.getSimpleName + _topNum

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
            val conn = driver.getConnector("user_db")

            val sql = "SELECT `value` v, count(`value`) n FROM user_personal where `type`='%s' and `name`='tag' and status=1 group by v order by n desc limit %d;".format(_type, _topNum)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()) {
                var tmpMap = Map[String, String]()
                tmpMap = tmpMap + ("tag" -> rs.getString(1))
                tmpMap = tmpMap + ("num" -> rs.getString(2))
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