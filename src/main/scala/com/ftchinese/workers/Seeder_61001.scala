package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.HBaseDriver
import com.wanbo.easyapi.server.lib._
import com.wanbo.easyapi.server.messages.CacheUpdate
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
 * Get uuid by cookieId.
 * Created by wanbo on 2015/4/17.
 */
final class Seeder_61001 extends Seeder with ISeeder {

    name = "61001"

    driver = new HBaseDriver

    private var _cookieId = ""

    private val log = LoggerFactory.getLogger(classOf[Seeder_61001])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {

            val cookieId = seed.getOrElse("cookieid", "").toString

            if(cookieId == "")
                throw new EasyException("20001")

            _cookieId = cookieId

            // Cache
            val cache_name = this.getClass.getSimpleName + _cookieId

            val cacher = new CacheManager(conf = _conf, expire = 604800)
            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && (cacheData.oelement.get("errorcode").get == "0" || cacheData.oelement.get("errorcode").get == "20101") && !isUpdateCache) {

                if(cacheData.oelement.getOrElse("errorcode", "-1") == "20101") {
                    throw new EasyException("20101")
                }

                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                if(isUpdateCache) {

                    if(cacheData != null && cacheData.oelement.getOrElse("errorcode", "-1") == "0" && cacheData.oelement.getOrElse("ttl", "-1").toInt > 10){
                        log.info("----------- No need to update cache, because expiry time more than 10s.")
                    } else {
                        log.info("----------- Ready to update cache.")
                        updateCache(cacher, cache_name)
                    }

                } else {
                    log.info("----------- Push a CacheUpdate message to queue.")
                    MessageQ.push("UpdateCache", CacheUpdate(name, seed))

                    // Write a cache data as the lock which can prevent more threads to update cache.
                    val cache_data = new EasyOutput
                    cache_data.odata = List[Map[String, Any]]()

                    cache_data.oelement = cache_data.oelement.updated("errorcode", "20101")
                    cacher.cacheData(cache_name, cache_data, 10)

                    throw new EasyException("20101")
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

    private def updateCache(cacheManager: CacheManager, cache_name: String): List[Map[String, Any]] ={

        var dataList = List[Map[String, Any]]()

        val uuid = onDBHandle()

        if (uuid == "")
            throw new EasyException("20100")
        else {
            val cache_data = new EasyOutput

            dataList = dataList :+ Map("uuid" -> uuid)

            cache_data.odata = dataList

            cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
            cacheManager.cacheData(cache_name, cache_data)
        }

        dataList
    }

    override protected def onDBHandle(): String = {
        var uuid = ""

        try {

            val driver = this.driver.asInstanceOf[HBaseDriver]
            val conn = driver.getConnector("")
            val table = conn.getTable(TableName.valueOf("user_cookieids"))

            log.info("Query ---------- cookieid:" + _cookieId)

            val get = new Get(Bytes.toBytes(_cookieId))

            val result = table.get(get)

            val uuidBytes = result.getValue(Bytes.toBytes("c"), Bytes.toBytes("uuid"))

            if(uuidBytes != null)
                uuid = new String(uuidBytes)

            table.close()
            conn.close()
        } catch {
            case e: Exception =>
                throw e
        }

        uuid
    }
}
