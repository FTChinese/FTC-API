package com.ftchinese.workers

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ftchinese.utils.Utils
import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MongoDriver
import com.wanbo.easyapi.server.lib._
import org.mongodb.scala._
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Recommend stories for users. (Base on access history)
 * Created by wanbo on 2015/4/17.
 */
final class Seeder_61009 extends Seeder with ISeeder {

    name = "61009"

    driver = new MongoDriver

    private var _primeKey = ""

    private val cache_time = 3600

    private val log = LoggerFactory.getLogger(classOf[Seeder_61009])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        val startRunTime = System.currentTimeMillis()

        try {

            val uuId = seed.getOrElse("uuid", "").toString
            val cookieId = seed.getOrElse("cookieid", "").toString

            if (uuId == "" && cookieId == ""){
                throw new EasyException("20001")
            }

            if (uuId == "") {

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
            } else
                _primeKey = uuId

            if(seed.contains("isUpdateCache") && seed.getOrElse("isUpdateCache", "") == "isUpdateCache")
                isUpdateCache = true

            // Cache
            val cache_name = this.getClass.getSimpleName + _primeKey

            val cacher = new CacheManager(conf = _conf, expire = cache_time)
            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                dataList = onDBHandle()

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
                            tmpMap = tmpMap + ("storyid" -> storyId)
                            tmpMap = tmpMap + ("cheadline" -> obj.getString("cheadline"))
                            tmpMap = tmpMap + ("piclink" -> picLink)
                            tmpMap = tmpMap + ("rating" -> obj.getString("rating").toDouble)
                            dataList = dataList :+ tmpMap
                        }
                    }
                })
            } else {
                throw new EasyException("20100")
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
