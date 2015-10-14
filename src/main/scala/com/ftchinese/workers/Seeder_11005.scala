package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory

/**
 * Get stories by channel id.
 * Created by wanbo on 2015/10/10.
 */
final class Seeder_11005 extends Seeder with ISeeder {

    name = "11005"

    driver = new MysqlDriver

    private val log = LoggerFactory.getLogger(classOf[Seeder_11005])

    private var _chanelId: String = ""
    private var _chanelCode: String = ""

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {

            val startRunTime = System.currentTimeMillis()

            val chaId: String = seed.getOrElse("id", "").toString

            if (chaId != "" && !chaId.forall(_.isDigit))
                throw new EasyException("20001")

            _chanelId = chaId

            val chaCode: String = seed.getOrElse("code", "").toString

            val pattern = "^[A-Za-z0-9-_]+$".r

            if(pattern.findAllIn(chaCode).size < 1)
                throw new EasyException("20001")

            _chanelCode = chaCode

            // Cache
            val cache_name = this.getClass.getSimpleName + _chanelId + _chanelCode
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

            var sql = ""

            if(_chanelId != "")
                sql = "select id,reid,`type`,`category`,haschildren from channel where `id` = '%s' and `type` = 'channel' and visible = 1 limit 1;".format(_chanelId)
            else
                sql = "select id,reid,`type`,`category`,haschildren from channel where `code` = '%s' and `type` = 'channel' and visible = 1 limit 1;".format(_chanelCode)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                var tmpMap = Map[String, String]()
                tmpMap = tmpMap + ("id" -> rs.getString(1))
                tmpMap = tmpMap + ("reid" -> rs.getString(2))
                tmpMap = tmpMap + ("type" -> rs.getString(3))
                tmpMap = tmpMap + ("category" -> rs.getString(4))
                tmpMap = tmpMap + ("haschildren" -> rs.getString(5))

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