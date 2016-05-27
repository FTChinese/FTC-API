package com.ftchinese.workers

import com.ftchinese.utils.ContentTypes
import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get content by tags.
 * Created by wanbo on 2016/05/10.
 */
final class Seeder_10005 extends Seeder with ISeeder {

    name = "10005"

    driver = new MysqlDriver

    private var _tags: Set[String] = Set()
    private var _type: Int = -1
    private var _pageIndex: Int = 1
    private var _pageSize: Int = 10
    private var _pageTotal: Int = 0
    private var _itemTotal: Int = 0

    private val log = LoggerFactory.getLogger(classOf[Seeder_10005])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val tags: String = seed.getOrElse("tags", "").toString

            val tmpTags = tags.split(",").map(_.trim).distinct

            tmpTags.foreach(tag => {
                if(tag.length < 2 || tag.length > 10)
                    throw new EasyException("20001")
            })

            _tags = tmpTags.toSet

            val contentType = seed.getOrElse("type", "-1").toString
            if (contentType != null && contentType.forall(_.isDigit) && contentType.toInt > 0){
                _type = contentType.toInt
            }

            // Page
            val pageIndex: String = seed.getOrElse("pageindex", "1").toString
            val pageSize: String = seed.getOrElse("pagesize", "10").toString

            if (pageIndex != null && !pageIndex.forall(_.isDigit))
                throw new EasyException("20001")

            if (pageSize != null && !pageSize.forall(_.isDigit))
                throw new EasyException("20001")

            _pageIndex = pageIndex.toInt
            _pageSize = pageSize.toInt

            if(_pageIndex > 100)
                _pageIndex = 100
            if(_pageIndex < 1)
                _pageIndex = 1
            if(_pageSize > 100)
                _pageSize = 100
            if(_pageSize < 1)
                _pageSize = 1

            // Cache
            val cache_name = this.getClass.getSimpleName + _tags.hashCode() + _type + _pageIndex + _pageSize

            val cacher = new CacheManager(_conf, expire = 300)
            val cacheData = cacher.cacheData(cache_name)

            if (cacheData != null && cacheData.oelement.get("errorcode").get == "0" && !isUpdateCache) {
                dataList = cacheData.odata
                fruits.oelement = fruits.oelement + ("fromcache" -> "true") + ("ttl" -> cacher.ttl.toString)
                fruits.oelement = fruits.oelement + ("itemtotal" -> cacheData.oelement.getOrElse("itemtotal", "0"))
                fruits.oelement = fruits.oelement + ("pagetotal" -> cacheData.oelement.getOrElse("pagetotal", "0"))
            } else {

                dataList = onDBHandle()

                if (dataList.size < 1)
                    throw new EasyException("20100")
                else {
                    val cache_data = new EasyOutput

                    cache_data.odata = dataList

                    fruits.oelement = fruits.oelement.updated("itemtotal", _itemTotal.toString)
                    cache_data.oelement = cache_data.oelement.updated("itemtotal", _itemTotal.toString)

                    fruits.oelement = fruits.oelement.updated("pagetotal", _pageTotal.toString)
                    cache_data.oelement = cache_data.oelement.updated("pagetotal", _pageTotal.toString)

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
    override protected def onDBHandle(): List[Map[String, Any]] = {
        var dataList = List[Map[String, Any]]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            var where = ""

            if(_type == -1) {
                where = "WHERE tag in ('" + _tags.mkString("','") + "')"
            } else {
                where = "WHERE tag in ('" + _tags.mkString("','") + "') and `type`=%d".format(_type)
            }

            val sql = "SELECT `tag`,`contentid`,`type`,`cheadline`,`clead`,`pubdate` FROM `tag_content` " + where + " order by pubdate desc, contentid desc limit %d,%d;".format((_pageIndex - 1) * _pageSize + 1, _pageSize)

            //log.info("SQL--------:" + sql)
            
            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()) {
                var tmpMap = Map[String, String]()
                tmpMap = tmpMap + ("tag" -> rs.getString(1))
                tmpMap = tmpMap + ("id" -> rs.getString(2))
                tmpMap = tmpMap + ("itemtype" -> ContentTypes.name(rs.getInt(3)))
                tmpMap = tmpMap + ("cheadline" -> rs.getString(4))
                tmpMap = tmpMap + ("clead" -> rs.getString(5))
                tmpMap = tmpMap + ("pubdate" -> rs.getString(6))

                dataList = dataList :+ tmpMap
            }

            rs.close()
            ps.close()

            val tps = conn.prepareStatement("SELECT count(contentid) totalnum FROM `tag_content` " + where)
            val trs = tps.executeQuery()

            if(trs.last()) {
                _itemTotal = trs.getInt(1)
                if(_itemTotal > 0)
                    _pageTotal = math.ceil(_itemTotal / _pageSize).toInt
            }

            trs.close()
            tps.close()

            conn.close()

        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }

}