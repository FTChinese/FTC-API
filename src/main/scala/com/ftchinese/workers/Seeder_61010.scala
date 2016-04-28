package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.HBaseDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.apache.hadoop.hbase.client.{Delete, HTable, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
 * Working for update data of story recommendation (61009).
 * Created by wanbo on 2015/4/17.
 */
final class Seeder_61010 extends Seeder with ISeeder {

    name = "61010"

    driver = new HBaseDriver

    private var _storyId = ""
    private var _primeKey = ""

    private val log = LoggerFactory.getLogger(classOf[Seeder_61010])

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
            else
                throw new EasyException("20001")

            // Cache
            val cache_name = "Seeder_61009" + _primeKey

            val cacher = new CacheManager(conf = _conf, expire = 21600)
            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && (cacheData.oelement.get("errorcode").get == "0") && !isUpdateCache) {
                var notExist = true
                cacheData.odata.foreach(x => {
                    log.info("storyid in cache:" + x.getOrElse("storyid", ""))
                    if(x.getOrElse("storyid", "") == _storyId)
                        notExist = false
                })
                log.info("Current story id:" + _storyId)

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
            cacheManager.cacheData(cache_name, cache_data, 21600)
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

            val driver = this.driver.asInstanceOf[HBaseDriver]

            val table = new HTable(driver.getHConf, Bytes.toBytes("user_recommend_stories"))

            // Delete the current story

            val rowKey = _primeKey + "#" + _storyId.toInt.toString

            val d = new Delete(Bytes.toBytes(rowKey))

            d.deleteFamily(Bytes.toBytes("a"))
            d.deleteFamily(Bytes.toBytes("c"))

            table.delete(d)

            // Read new data

            val scan = new Scan()
            scan.addFamily(Bytes.toBytes("c"))
            scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("storyid"))
            scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("cheadline"))
            scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("rating"))

            log.info("Query ---------- primeKey:" + _primeKey)

            if(_primeKey != "") {
                scan.setStartRow(Bytes.toBytes(_primeKey))
                scan.setFilter(new PrefixFilter(Bytes.toBytes(_primeKey)))
                scan.setStopRow(Bytes.toBytes(_primeKey + 'Z'))
            }

            log.info("Start row ------:" + new String(scan.getStartRow))
            log.info("Stop row ------:" + new String(scan.getStopRow))

            val retScanner = table.getScanner(scan)

            var result = retScanner.next()
            while (result != null) {

                val rowKey = new String(result.getRow)
                val fields = rowKey.split("#")
                val id = fields(0)

                log.info("Row key ------:" + rowKey)

                if(_primeKey == id) {

                    val s = result.getValue(Bytes.toBytes("c"), Bytes.toBytes("storyid"))
                    val c = result.getValue(Bytes.toBytes("c"), Bytes.toBytes("cheadline"))
                    val r = result.getValue(Bytes.toBytes("c"), Bytes.toBytes("rating"))

                    if (s != null && c != null && r != null) {
                        var rating: Double = 0
                        try {
                            rating = new String(r).toDouble
                        } catch {
                            case e: Exception =>
                            // Ignore
                        }
                        dataList = dataList :+(new String(s), new String(c), rating)
                    }
                }

                result = retScanner.next()
            }

            table.close()
        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }
}
