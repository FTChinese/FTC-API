package com.ftchinese.workers

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ftchinese.utils.Utils
import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MongoDriver
import com.wanbo.easyapi.server.lib._
import org.mongodb.scala.Document
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Recommend stories for users. (Base on access history)
 * Created by wanbo on 2016/7/6.
 */
final class Seeder_61008 extends Seeder with ISeeder {

    name = "61008"

    driver = new MongoDriver

    private var _primeKey = ""

    private val cache_time = 30

    private val log = LoggerFactory.getLogger(classOf[Seeder_61008])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        val startRunTime = System.currentTimeMillis()

        try {

            val uuId = seed.getOrElse("uuid", "").toString
            val cookieId = seed.getOrElse("cookieid", "").toString

            if(uuId == "" && cookieId == ""){
                _primeKey = ""
            } else if(uuId == ""){
                // Get uuid By cookieId.
                val uidData = manager.transform("61001", Map("cookieid" -> cookieId))

                if (uidData.oelement.get("errorcode").get == "0") {
                    val tmpUUID = uidData.odata.last.getOrElse("uuid", "").toString
                    if (tmpUUID != "")
                        _primeKey = tmpUUID
                    else
                        _primeKey = cookieId
                } else {
                    _primeKey = cookieId
                }
            } else {
                _primeKey = uuId
            }



            // Cache
            val cache_name = this.getClass.getSimpleName + _primeKey

            val cacher = new CacheManager(conf = _conf, expire = cache_time)
            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                var uniqueIds = Set[String]()

                val data_61008 = onDBHandle()
                var distinct_61008 = List[Map[String, Any]]()

                data_61008.foreach(x => {
                    x.get("storyid").foreach(id => {
                        if(!uniqueIds.contains(id.toString)){
                            distinct_61008 = distinct_61008 :+ x
                            uniqueIds = uniqueIds + id.toString
                        }
                    })
                })


                val hotData = manager.transform("61006", Map[String, Any]())

                val data_61006 = hotData.odata

                var distinct_61006 = List[Map[String, Any]]()
                data_61006.foreach(x => {
                    x.get("storyid").foreach(id => {
                        if(!uniqueIds.contains(id.toString)){
                            distinct_61006 = distinct_61006 :+ x.updated("t", 2)
                            uniqueIds = uniqueIds + id.toString
                        }
                    })
                })

                // Mix all the distinct data into a new List.
                val mixedData = List(distinct_61008, distinct_61006)

                val regular = List(7, 3)


                dataList = Utils.dataBalance[Map[String, Any]](mixedData, regular)


                val cache_data = new EasyOutput
                cache_data.odata = dataList
                cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
                cacher.cacheData(cache_name, cache_data, cache_time)

            }
            cacher.close()

            fruits.oelement = fruits.oelement.updated("errorcode", "0").+("duration" -> (System.currentTimeMillis() - startRunTime).toString)
            fruits.odata = dataList.slice(0, 10)
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

    private def getStoryPic(storyId: String): String ={
        var imgLink = ""

        val imgData = manager.transform("10006", Map("storyid" -> storyId))

        if (imgData.oelement.get("errorcode").get == "0") {
            val cover = imgData.odata.filter(x => x.getOrElse("otype", "")  == "Cover")
            val other = imgData.odata.filter(x => x.getOrElse("otype", "")  == "Other" || x.getOrElse("otype", "")  == "BigButton")

            if(cover.nonEmpty){
                imgLink = Utils.formatRealImgUrl(cover.head.getOrElse("olink", "").toString)
            } else if (other.nonEmpty) {
                imgLink = Utils.formatRealImgUrl(other.head.getOrElse("olink", "").toString)
            }
        }

        imgLink
    }

    override protected def onDBHandle(): List[Map[String, Any]] = {
        var dataList = List[Map[String, Any]]()

        try {

            val driver = this.driver.asInstanceOf[MongoDriver]

            val coll = driver.getCollection("recommend", "recommend_stories")

            //log.info("Query ---------- primeKey:" + _primeKey)

            val retDocument = Await.result(coll.find(Document("_id" -> _primeKey)).first().toFuture(), Duration(10, TimeUnit.SECONDS))

            if(retDocument.nonEmpty){
                val stories = retDocument.head.get("stories")
                stories.foreach(s => {
                    val jsonString = s.asString().getValue

                    val storyArr = JSON.parseArray(jsonString)

                    val iterator = storyArr.iterator()
                    while (iterator.hasNext) {

                        val obj = iterator.next().asInstanceOf[JSONObject]
                        val storyId = Utils.formatStoryId(obj.getString("storyid"))
                        val picLink = getStoryPic(storyId)
                        if(picLink.nonEmpty) {
                            var tmpMap = Map[String, Any]()
                            tmpMap = tmpMap + ("storyid" -> obj.getString("storyid"))
                            tmpMap = tmpMap + ("cheadline" -> obj.getString("cheadline"))
                            tmpMap = tmpMap + ("piclink" -> picLink)
                            tmpMap = tmpMap + ("rating" -> obj.getString("rating").toDouble)
                            tmpMap = tmpMap + ("t" -> 1)
                            dataList = dataList :+ tmpMap
                        }
                    }
                })
            } else {
                //throw new EasyException("20100")
            }

            // close
            driver.close()
        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }
}
