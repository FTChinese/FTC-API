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
 * Related stories.
 * Created by wanbo on 2016/9/12.
 */
final class Seeder_61007 extends Seeder with ISeeder {

    name = "61007"

    driver = new MongoDriver

    private var _storyId = ""

    private val cache_time = 86400

    private val log = LoggerFactory.getLogger(classOf[Seeder_61007])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        val startRunTime = System.currentTimeMillis()

        try {

            val storyId: String = seed.getOrElse("storyid", "").toString.trim

            if(storyId == "" || !storyId.forall(_.isDigit)) {
                throw new EasyException("20001")
            }

            _storyId = storyId


            // Cache
            val cache_name = this.getClass.getSimpleName + _storyId

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
                    data.foreach { case (storyId: String, weight: Double) =>

                        var obj = Map[String, Any]()

                        val cHeadLine = getStoryInfo(storyId)
                        val picLink = getStoryPic(storyId)

                        if (cHeadLine.nonEmpty && picLink.nonEmpty) {

                            obj = obj + ("storyid" -> storyId)
                            obj = obj + ("cheadline" -> cHeadLine)
                            obj = obj + ("piclink" -> picLink)
                            obj = obj + ("weight" -> weight)
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
                imgLink = Utils.formatRealImgUrl(cover.head.getOrElse("olink", "").toString)
            } else if (other.nonEmpty) {
                imgLink = Utils.formatRealImgUrl(other.head.getOrElse("olink", "").toString)
            }
        }

        imgLink
    }

    override protected def onDBHandle(): List[(String, Double)] = {
        var dataList = List[(String, Double)]()

        try {

            val driver = this.driver.asInstanceOf[MongoDriver]

            val coll = driver.getCollection("recommend", "related_stories")

            //log.info("Query ---------- primeKey:" + _primeKey)

            val retDocument = Await.result(coll.find(Document("_id" -> _storyId)).first().toFuture(), Duration(10, TimeUnit.SECONDS))

            if(retDocument.nonEmpty){
                val stories = retDocument.head.get("stories")
                stories.foreach(s => {
                    val jsonString = s.asString().getValue

                    val storyArr = JSON.parseArray(jsonString)

                    val iterator = storyArr.iterator()
                    while (iterator.hasNext) {
                        val obj = iterator.next().asInstanceOf[JSONObject]
                        val s = obj.getString("storyid")
                        val p = obj.getString("weight")

                        dataList = dataList :+ (s, p.toDouble)
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
