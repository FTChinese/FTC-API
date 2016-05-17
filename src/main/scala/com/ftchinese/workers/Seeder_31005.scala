package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get content according MyFT configurations.
 * Created by wanbo on 2016/05/10.
 */
final class Seeder_31005 extends Seeder with ISeeder {

    name = "31005"

    driver = new MysqlDriver

    private var _uuId: String = ""
    private val _type: String = "myft"
    private var _topNum: Int = 10

    private var _myConf = Map[String, Set[String]]()

    private val log = LoggerFactory.getLogger(classOf[Seeder_31005])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val uuId: String = seed.getOrElse("uuid", "").toString

            if(uuId.isEmpty)
                throw new EasyException("50002")

            if(uuId.length < 3 || uuId.length > 36)
                throw new EasyException("20001")

            _uuId = uuId.trim

            val num = seed.getOrElse("topnum", "10").toString
            if (num == null || !num.forall(_.isDigit)){
                _topNum = 10
            } else {
                _topNum = num.toInt
            }

            if(_topNum > 30)
                _topNum = 30

            // Cache
            val cache_name = this.getClass.getSimpleName + _uuId + _topNum

            val cacher = new CacheManager(_conf, expire = 10)
            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                val myFTData = manager.transform("31004", Map(("uuid", _uuId)))

                val errorCode = myFTData.oelement.get("errorcode").get

                if(errorCode == "0"){

                    _myConf = myFTData.odata.map(x => (x.getOrElse("type", "").toString, x.getOrElse("value", "").toString)).groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).toSet))


                    // Filter content
                    dataList = onDBHandle()

                    if (dataList.size < 1)
                        throw new EasyException("20100")
                    else {
                        val cache_data = new EasyOutput
                        cache_data.odata = dataList

                        cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
                        cacher.cacheData(cache_name, cache_data)
                    }
                    // Filter content

                } else {
                    throw new EasyException(errorCode)
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
    override protected def onDBHandle(): List[Map[String, Any]] = {
        var dataList = List[Map[String, Any]]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            if(_myConf.nonEmpty) {

                _myConf.foreach ( x => {
                    x._1 match {
                        case "tag" =>

                            val sql = "SELECT `tag`,`contentid`,`type`,`cheadline`,`clead`,`pubdate` FROM `tag_content` WHERE tag in ('" + x._2.mkString("','") + "') order by pubdate desc, contentid desc limit %d;".format(_topNum)

                            //log.info("SQL--------:" + sql)

                            val ps = conn.prepareStatement(sql)
                            val rs = ps.executeQuery()

                            while (rs.next()){
                                var tmpMap = Map[String, String]()
                                tmpMap = tmpMap + ("tag" -> rs.getString(1))
                                tmpMap = tmpMap + ("id" -> rs.getString(2))
                                tmpMap = tmpMap + ("itemtype" -> contentType(rs.getString(3)))
                                tmpMap = tmpMap + ("cheadline" -> rs.getString(4))
                                tmpMap = tmpMap + ("clead" -> rs.getString(5))
                                tmpMap = tmpMap + ("pubdate" -> rs.getString(6))

                                dataList = dataList :+ tmpMap
                            }

                            rs.close()
                            ps.close()

                        case _ =>
                            // Nothing to do.
                    }
                })

            } else {
                throw new EasyException("20100")
            }

            conn.close()

        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }

    private def contentType(t: String): String = t match {
        case "1" => "story"
        case "2" => "video"
        case "3" => "interactive"
        case "4" => "photonews"
        case _ => ""
    }

}