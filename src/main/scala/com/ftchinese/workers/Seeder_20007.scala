package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get the latest data of Polling or Debate.
 * Created by wanbo on 2016/01/07.
 */
final class Seeder_20007 extends Seeder with ISeeder {

    name = "20007"

    driver = new MysqlDriver

    private var _type: Int = 1
    private var _num: Int = 1

    private val log = LoggerFactory.getLogger(classOf[Seeder_20007])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val inType: String = seed.getOrElse("type", "").toString
            val inNum: String = seed.getOrElse("num", "").toString

            try{

                if(inType != "") {
                    val tmpType = inType.toInt

                    if(tmpType == 2)
                        _type = tmpType
                }

                if(inNum != "") {
                    val tmpNum = inNum.toInt

                    if(tmpNum > 0 && tmpNum < 100)
                        _num = tmpNum
                }

            } catch {
                case _: Exception => throw new EasyException("20001")
            }

            // Cache
            val cache_name = this.getClass.getSimpleName + _type + _num

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

            var tableName = "poll_info"
            if(_type == 2)
                tableName = "debate_info"

            val sql = "SELECT info_id `id`,info_subject `subject`,info_lead `lead`,info_img `image`,info_starttime starttime,info_endtime endtime " +
                "FROM %s where info_isdeleted=0 and info_visible=1 and info_starttime < unix_timestamp() and unix_timestamp() < (info_endtime+86400) ".format(tableName) +
                "order by info_id desc limit %d;".format(_num)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            val metaData = ps.getMetaData
            val columnCount = metaData.getColumnCount
            while (rs.next()) {
                var tmpMap = Map[String, String]()
                for(i <- Range(1, columnCount + 1)) {
                    val rowKey = metaData.getColumnLabel(i)
                    if(rowKey == "image")
                        tmpMap = tmpMap + (rowKey -> rs.getString(i).replaceFirst("/upload/", ""))
                    else
                        tmpMap = tmpMap + (rowKey -> rs.getString(i))
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