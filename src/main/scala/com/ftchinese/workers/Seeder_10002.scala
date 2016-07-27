package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory

/**
 * Get story info by id.
 * Created by wanbo on 15/10/10.
 */
final class Seeder_10002 extends Seeder with ISeeder {

    name = "10002"

    driver = new MysqlDriver

    private var _idSet: Set[String] = Set()
    private var _type: String = "info"

    private val log = LoggerFactory.getLogger(classOf[Seeder_10002])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val storyIdStr: String = seed.getOrElse("storyid", "").toString

            val storyArr = storyIdStr.split(",")

            if(storyArr.nonEmpty) {
                storyArr.foreach(x => {
                    if(x != "" && x.forall(_.isDigit))
                        _idSet += x
                })
            } else {
                throw new EasyException("20001")
            }

            if(_idSet.size < 1)
                throw new EasyException("20001")

            val infoType = seed.getOrElse("type", "info").toString
            if (infoType == "all"){
                _type = "all"
            }

            // Cache
            val cache_name = this.getClass.getSimpleName + _idSet.hashCode() + _type

            val cacher = new CacheManager(conf = _conf, expire = 600)

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
            val conn = driver.getConnector("cmstmp01")

            var fields = Set("id", "cheadline", "cauthor")

            _type match {
                case "all" =>
                    fields = Set("*")
                case _ => // Ignore
            }

            val sql = "SELECT %s FROM story where `publish_status` = 'publish' and id in (%s)".format(fields.mkString(","), _idSet.mkString("'", "','", "'"))

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            if(_type == "all") {
                val metaData = ps.getMetaData
                val columnCount = metaData.getColumnCount
                while (rs.next()) {
                    var tmpMap = Map[String, String]()
                    for(i <- Range(1, columnCount + 1)) {
                        tmpMap = tmpMap + (metaData.getColumnLabel(i) -> rs.getString(i))
                    }

                    dataList = dataList :+ tmpMap
                }
            } else {
                while (rs.next()) {
                    var tmpMap = Map[String, String]()
                    tmpMap = tmpMap + ("id" -> rs.getString(1))
                    tmpMap = tmpMap + ("cheadline" -> rs.getString(2))
                    tmpMap = tmpMap + ("cauthor" -> rs.getString(3))

                    dataList = dataList :+ tmpMap
                }
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