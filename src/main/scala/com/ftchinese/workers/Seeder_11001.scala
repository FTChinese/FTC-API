package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Number 11001 seeder
 * Created by wanbo on 2015/4/28.
 */
final class Seeder_11001 extends Seeder with ISeeder {

    name = "11001"

    driver = new MysqlDriver

    private val log = LoggerFactory.getLogger(classOf[Seeder_11001])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val startRunTime = System.currentTimeMillis()

            // Cache
            val cache_name = this.getClass.getSimpleName

            val cacher = new CacheManager(conf = _conf, expire = 86400)

            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
            } else {

                val data = onDBHandle()

                if (data.size < 1)
                    throw new EasyException("20100")
                else {
                    val cache_data = new EasyOutput
                    cache_data.odata = List[Map[String, Any]]()

                    val treeData = list2tree("0", data)
                    dataList = treeData
                    cache_data.odata = treeData

                    cache_data.oelement = cache_data.oelement.updated("errorcode", "0")
                    cacher.cacheData(cache_name, cache_data)
                }
            }
            cacher.close()

            fruits.oelement = fruits.oelement.updated("errorcode", "0").+("duration" -> (System.currentTimeMillis() - startRunTime).toString)
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
    override protected def onDBHandle(): List[(String, String, String, String, String, String, String)] = {
        var dataList = List[(String, String, String, String, String, String, String)]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            val sql = "SELECT id,reid,`code`,`name`,`type`,`link`,haschildren FROM `channel`  ORDER BY priority,`reid` DESC,`id`"

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                dataList = dataList :+ (rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7))
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

    private def list2tree(reId: String, list: List[(String, String, String, String, String, String, String)]): List[Map[String, Any]] ={
        var jList = List[Map[String, Any]]()
        val aList = list.filter(_._2 == reId)
        val bList = list.diff(aList)

        if(bList.size > 0) {
            aList.foreach(x => {
                var tmpMap = Map[String, Any]()

                tmpMap = tmpMap + ("id" -> x._1)
                tmpMap = tmpMap + ("reid" -> x._2)
                tmpMap = tmpMap + ("code" -> x._3)
                tmpMap = tmpMap + ("name" -> x._4)
                tmpMap = tmpMap + ("type" -> x._5)

                if(x._5 != "link" && x._3 != ""){
                    tmpMap = tmpMap + ("link" -> "/%s/%s.html".format(x._5, x._3))
                } else {
                    tmpMap = tmpMap + ("link" -> x._6)
                }
                tmpMap = tmpMap + ("haschildren" -> x._7)
                tmpMap = tmpMap + ("children" -> list2tree(x._1, bList))
                jList = jList :+ tmpMap
            })
        }
        jList
    }
}