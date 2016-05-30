package com.ftchinese.workers

import com.ftchinese.utils.Diffusing
import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib._
import com.wanbo.easyapi.server.messages.CacheUpdate
import org.slf4j.LoggerFactory

/**
 * Recommend tags for users. (Base on following tags data)
 * Created by wanbo on 2016/5/30.
 */
final class Seeder_61020 extends Seeder with ISeeder {

    name = "61020"

    driver = new MysqlDriver

    private var _primeKey = ""
    private val _type: String = "myft"
    private val _topNum: Int = 10

    private val log = LoggerFactory.getLogger(classOf[Seeder_61020])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        val startRunTime = System.currentTimeMillis()

        try {

            val uuId: String = seed.getOrElse("uuid", "").toString.trim

            if(uuId.isEmpty)
                throw new EasyException("50002")

            if(uuId.length < 3 || uuId.length > 36)
                throw new EasyException("20001")

            _primeKey = uuId

            // Cache
            val cache_name = this.getClass.getSimpleName + _primeKey

            val cacher = new CacheManager(conf = _conf, expire = 600)
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

            fruits.oelement = fruits.oelement.updated("errorcode", "0").+("duration" -> (System.currentTimeMillis() - startRunTime).toString)
            fruits.odata = dataList.slice(0, _topNum)
        } catch {
            case ee: EasyException =>
                fruits.oelement = fruits.oelement.updated("errorcode", ee.getCode).+("duration" -> (System.currentTimeMillis() - startRunTime).toString)
            case e: Exception =>
                log.error("Seeder has exception:", e)
                fruits.oelement = fruits.oelement.updated("errorcode", "-1")
                fruits.oelement = fruits.oelement.updated("errormsg", e.getMessage)
        }

        fruits
    }

    private def updateCache(cacheManager: CacheManager, cache_name: String): List[Map[String, Any]] ={

        var dataList = List[Map[String, Any]]()

        val data = onDBHandle()

        if (data.size < 1)
            throw new EasyException("20100")
        else {
            val cache_data = new EasyOutput
            cache_data.odata = List[Map[String, Any]]()

            val diffusing = new Diffusing()

            diffusing.fit(data)

            val predictData = diffusing.predictMD(_primeKey)

            predictData.foreach(x => {
                var obj = Map[String, Any]()

                obj = obj + ("tag" -> x._1)
                obj = obj + ("weight" -> x._2)
                dataList = dataList :+ obj

                cache_data.odata = cache_data.odata :+ obj
            })
            cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
            cacheManager.cacheData(cache_name, cache_data, 21600)
        }

        //cacheManager.close()

        dataList
    }

    override protected def onDBHandle(): List[(String, String)] = {
        var dataList = List[(String, String)]()

        try {

            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("user_db")

            val sql = "SELECT `uuid`, `value` FROM user_db.user_personal where `type`='%s' and `name`='tag' and status=1 order by last_update desc limit 1000;".format(_type)

            //log.info("SQL--------:" + sql)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()) {
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
