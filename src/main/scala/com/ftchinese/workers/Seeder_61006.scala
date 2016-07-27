package com.ftchinese.workers

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MongoDriver
import com.wanbo.easyapi.server.lib._
import org.mongodb.scala.Document
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Real-time hot story over a period of time.
 * Created by wanbo on 2016/7/27.
 */
final class Seeder_61006 extends Seeder with ISeeder {

    name = "61006"

    driver = new MongoDriver

    private val _primeKey = "hot_story_period"

    private val cache_time = 600

    private val log = LoggerFactory.getLogger(classOf[Seeder_61006])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        val startRunTime = System.currentTimeMillis()

        try {

            // No input parameters.

            // Cache
            val cache_name = this.getClass.getSimpleName + _primeKey

            val cacher = new CacheManager(conf = _conf, expire = cache_time)
            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                val data = onDBHandle()

                val cache_data = new EasyOutput
                cache_data.odata = List[Map[String, Any]]()

                if(data.nonEmpty){
                    data.foreach { case (storyId: String, pv: Int) =>

                        var obj = Map[String, Any]()

                        val cHeadLine = getStoryInfo(storyId)
                        val picLink = getStoryPic(storyId)

                        if (cHeadLine.nonEmpty && picLink.nonEmpty) {

                            obj = obj + ("storyid" -> storyId)
                            obj = obj + ("cheadline" -> cHeadLine)
                            obj = obj + ("piclink" -> picLink)
                            obj = obj + ("pv" -> pv)
                        }

                        if (obj.nonEmpty)
                            dataList = dataList :+ obj
                    }
                } else {
                    throw new EasyException("20100")
                }

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

    private def getStoryInfo(storyId: String): String ={
        var headLine = ""

        val storyData = manager.transform("10002", Map("storyid" -> storyId))

        if (storyData.oelement.get("errorcode").get == "0") {
            val head_line = storyData.odata.head.getOrElse("cheadline", "").toString

            if(head_line.nonEmpty){
                headLine = head_line
            }
        }

        headLine
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

    override protected def onDBHandle(): List[(String, Int)] = {
        var dataList = List[(String, Int)]()

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
                        val s = obj.getString("storyid")
                        val p = obj.getString("pv")

                        dataList = dataList :+ (s, p.toInt)
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
