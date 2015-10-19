package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get custom email template.
 * Created by wanbo on 2015/10/14.
 */
final class Seeder_20011 extends Seeder with ISeeder {

    name = "20011"

    driver = new MysqlDriver

    private var _name: String = _

    private val log = LoggerFactory.getLogger(classOf[Seeder_20011])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val name: String = seed.getOrElse("name", "").toString

            if (name != "" || name.length > 45)
                throw new EasyException("20001")

            _name = name

            // Cache
            val cache_name = this.getClass.getSimpleName + _name

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
            val conn = driver.getConnector("analytic")


            val sql = "SELECT `title`,`ccode`,`desc`,`headimg`,`nav`,`ads`,`rail`,`main_content`,`supporting_content`,`articles`,`buttons`,`prize`,isshare`,`isdownapp` FROM conferencedb.dailymail_info WHERE `name`=%s and isdeleted=0 limit 1;".format(_name)

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