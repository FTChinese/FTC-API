package com.ftchinese.workers

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MongoDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import com.wanbo.easyapi.shared.common.Logging
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonInt64
import org.mongodb.scala.model.Filters

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Working for update data of story recommendation (61008).
 * Created by wanbo on 2015/4/17.
 */
final class Seeder_61010 extends Seeder with ISeeder with Logging {

    name = "61010"

    driver = new MongoDriver

    private var _storyId = ""
    private var _primeKey = ""

    private val cache_time = 3600

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        val startRunTime = System.currentTimeMillis()

        try {

            val uuId = seed.getOrElse("uuid", "").toString
            val cookieId = seed.getOrElse("cookieid", "").toString

            if (uuId == "" && cookieId == "")
                throw new EasyException("20001")

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

            val storyId = seed.getOrElse("storyid", "").toString

            if (storyId != null && !storyId.forall(_.isDigit))
                throw new EasyException("20001")

            if(storyId != "")
                _storyId = storyId

            if(seed.contains("isUpdateCache") && seed.getOrElse("isUpdateCache", "") == "isUpdateCache")
                isUpdateCache = true

            // Cache
            val cache_name = "Seeder_61008" + _primeKey

            val cacher = new CacheManager(conf = _conf, expire = cache_time)
            val cacheData = cacher.cacheData(cache_name)

            // Prevent the concurrent operation
            if (cacheData != null && (cacheData.oelement.get("errorcode").get == "0") && !isUpdateCache) {

                if(_storyId == ""){
                    throw new EasyException("20001")
                }

                var notExist = true
                cacheData.odata.foreach(x => {
                    //log.info("storyid in cache:" + x.getOrElse("storyid", ""))
                    if(x.getOrElse("storyid", "") == _storyId)
                        notExist = false
                })
                //log.info("Current story id:" + _storyId)

                // Maybe this one was already deleted.
                if(notExist)
                    throw new EasyException("20100")
            }

            // Create new thread to do rest work.
            val t = new Thread(){
                override def run(): Unit = {
                    try {

                        updateCache(cacher, cache_name)

                        //log.info("----------- Cache update successful.")
                    } catch {
                        case e: Exception =>
                            log.error("Cache update exception:", e)
                    }
                }
            }

            t.start()

            fruits.oelement = fruits.oelement.updated("errorcode", "0").+("duration" -> (System.currentTimeMillis() - startRunTime).toString)
            fruits.odata = List[Map[String, Any]]()
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

        val data = onDBHandle()

        if (data.size < 1)
            throw new EasyException("20100")
        else {
            val cache_data = new EasyOutput
            cache_data.odata = List[Map[String, Any]]()

            val sortData = data.sortBy(x => x._3)(Ordering.Double.reverse)
            sortData.slice(0, 15).foreach(x => {
                var obj = Map[String, Any]()

                val storyId = x._1.reverse.padTo(9, 0).reverse.mkString
                obj = obj + ("storyid" -> storyId)
                obj = obj + ("cheadline" -> x._2)
                obj = obj + ("piclink" -> getStoryPic(storyId))
                dataList = dataList :+ obj

                cache_data.odata = cache_data.odata :+ obj
            })
            cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
            cacheManager.cacheData(cache_name, cache_data, cache_time)
        }

        cacheManager.close()

        dataList
    }

    private def getStoryPic(storyId: String): String ={
        var imgLink = ""

        val imgData = manager.transform("10006", Map("storyid" -> storyId))

        if (imgData.oelement.get("errorcode").get == "0") {
            val cover = imgData.odata.filter(x => x.getOrElse("otype", "")  == "Cover")
            val other = imgData.odata.filter(x => x.getOrElse("otype", "")  == "Other" || x.getOrElse("otype", "")  == "BigButton")

            if(cover.nonEmpty){
                imgLink = cover.head.getOrElse("olink", "").toString.replaceFirst("/upload", "http://i.ftimg.net")
            } else if (other.nonEmpty) {
                imgLink = other.head.getOrElse("olink", "").toString.replaceFirst("/upload", "http://i.ftimg.net")
            }
        }

        imgLink
    }

    override protected def onDBHandle(): List[(String, String, Double)] = {
        var dataList = List[(String, String, Double)]()

        try {

            val driver = this.driver.asInstanceOf[MongoDriver]

            val coll = driver.getCollection("recommend", "recommend_stories")

            //log.info("Query ---------- primeKey:" + _primeKey)

            val retDocument = Await.result(coll.find(Document("_id" -> _primeKey)).first().toFuture(), Duration(10, TimeUnit.SECONDS))

            if(retDocument.nonEmpty){

                val storyList = new JSONArray()

                val stories = retDocument.head.get("stories")
                stories.foreach(s => {
                    val jsonString = s.asString().getValue

                    val storyArr = JSON.parseArray(jsonString)

                    val iterator = storyArr.iterator()
                    while (iterator.hasNext) {
                        val obj = iterator.next().asInstanceOf[JSONObject]
                        val s = obj.getString("storyid")
                        val c = obj.getString("cheadline")
                        val r = obj.getString("rating")

                        if(_storyId.toInt.toString != s) {
                            dataList = dataList :+(s, c, r.toDouble)
                            storyList.add(obj)
                        }
                    }
                })

                if(storyList.size() > 0){
                    val ret = Await.result(coll.replaceOne(Filters.eq("_id", _primeKey), Document("_id" -> _primeKey, "stories" -> storyList.toJSONString, "addtime" -> BsonInt64(System.currentTimeMillis()))).toFuture(), Duration(10, TimeUnit.SECONDS))

                    log.info("Records saved to MongoDB " + ret)
                }
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
