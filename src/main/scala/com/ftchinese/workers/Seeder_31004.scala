package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib._
import com.wanbo.easyapi.server.messages.CacheUpdate
import org.slf4j.LoggerFactory


/**
 * Query MyFT configurations.
 * Created by wanbo on 2016/04/08.
 */
final class Seeder_31004 extends Seeder with ISeeder {

    name = "31004"

    driver = new MysqlDriver

    private var _uuId: String = ""
    private val _type: String = "myft"

    private val log = LoggerFactory.getLogger(classOf[Seeder_31004])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val uuId: String = seed.getOrElse("uuid", "").toString

            if(uuId.isEmpty)
                throw new EasyException("50002")

            if(uuId.length < 3 || uuId.length > 36)
                throw new EasyException("20001")

            _uuId = uuId.trim


            // Cache
            val cache_name = this.getClass.getSimpleName + _uuId

            val cacher = new CacheManager(_conf, expire = 86400)
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

                    // Synchronization update the 31005 cache.
                    MessageQ.push("UpdateCache", CacheUpdate("31005", Map("uuid" -> _uuId, "topnum" -> 30)))
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
            val conn = driver.getConnector("user_db", writable = true)

            val sql = "SELECT `name`,`value` FROM `user_personal` WHERE `status` = 1 and `uuid`='%s' and `type`='%s';".format(_uuId, _type)

            // log.info("SQL--------:" + sql)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                var tmpMap = Map[String, String]()
                tmpMap = tmpMap + ("type" -> rs.getString(1))
                tmpMap = tmpMap + ("value" -> rs.getString(2))
                dataList = dataList :+ tmpMap
            }

            ps.close()

            conn.close()

        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }

}