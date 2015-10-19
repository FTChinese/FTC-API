package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get stories by column id. If the column id is empty, return all columns stories.
 * Created by wanbo on 2015/10/19.
 */
final class Seeder_10013 extends Seeder with ISeeder {

    name = "10013"

    driver = new MysqlDriver

    private var _idSet = Set[String]()
    private var _topNum: Int = 100

    private val log = LoggerFactory.getLogger(classOf[Seeder_10013])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val idStr = seed.getOrElse("id", "").toString

            val idArr = idStr.split(",")

            if(idArr.size > 0) {
                idArr.foreach(x => {
                    if(x != "" && x.forall(_.isDigit))
                        _idSet += x
                })
            }

            val num = seed.getOrElse("num", "100").toString
            if (num == null || !num.forall(_.isDigit)){
                _topNum = 100
            } else {
                _topNum = num.toInt
            }

            if(_topNum < 1 || _topNum > 100)
                _topNum = 100

            // Cache
            val cache_name = this.getClass.getSimpleName + _idSet.hashCode() + _topNum

            val cacher = new CacheManager(conf = _conf, expire = 0)

            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                dataList = onDBHandle()

                if (dataList.size < 1)
                    throw new EasyException("20100")
                else {

                    val colIds = dataList.map(_.getOrElse("column", "").toString).filter(_!="").mkString(",")
                    val columnData = manager.transform("10010", Map(("id", colIds)))

                    if(columnData.oelement.get("errorcode").get == "0"){
                        val mapData = dataList.map(x => (x.get("column").get, x))
                        dataList = List[Map[String, Any]]()
                        columnData.odata.foreach(x => {
                            mapData.foreach(y => {
                                if(x.getOrElse("id", "") == y._1){
                                    dataList = dataList :+ x ++ y._2
                                }
                            })
                        })
                    }

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

            var sql = ""
            if(_idSet.size > 0) {
                sql = "SELECT id,cheadline,cauthor,`column`,tag FROM story where `column` != '' and `column` in (%s) order by `pubdate` desc, `fileupdatetime` desc limit %d;".format(_idSet.mkString("'", "','", "'"), _topNum)
            } else {
                sql = "SELECT id,cheadline,cauthor,`column`,tag FROM story where `column` != '' order by `pubdate` desc, `fileupdatetime` desc limit %d;".format(_topNum)
            }

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            val metaData = ps.getMetaData
            val columnCount = metaData.getColumnCount
            while (rs.next()) {
                var tmpMap = Map[String, String]()

                for(i <- Range(1, columnCount + 1)) {
                    tmpMap = tmpMap + (metaData.getColumnLabel(i) -> rs.getString(i))
                }

                dataList = dataList :+ tmpMap
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