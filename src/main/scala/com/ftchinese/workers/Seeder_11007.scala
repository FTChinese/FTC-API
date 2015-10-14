package com.ftchinese.workers

import com.wanbo.easyapi.server.cache.CacheManager
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get videos by channel id.
 * Created by wanbo on 2015/6/26.
 */
final class Seeder_11007 extends Seeder with ISeeder {

    name = "11007"

    driver = new MysqlDriver

    private val log = LoggerFactory.getLogger(classOf[Seeder_11007])

    private var _chanelId: Int = 1
    private var _pageIndex: Int = 1
    private var _pageSize: Int = 10

    private var _pageTotal: Int = 0
    private var _itemTotal: Int = 0

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {

            val startRunTime = System.currentTimeMillis()

            val chaId: String = seed.getOrElse("id", "1").toString

            if (chaId != null && !chaId.forall(_.isDigit))
                throw new EasyException("20001")

            _chanelId = chaId.toInt

            if(_chanelId < 1)
                throw new EasyException("20001")

            if(_chanelId > 100)
                _chanelId = 100

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
            val cache_name = this.getClass.getSimpleName + _chanelId + _pageIndex + _pageSize
            val cacher = new CacheManager(conf = _conf, expire = 600)

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
    override protected def onDBHandle(): List[Map[String, String]] = {
        var dataList = List[Map[String, String]]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("cmstmp01")

            val sql = ("select s.`id`,s.`fileupdatetime`,s.`name`,s.`cheadline`,s.`cskylineheadline`,s.`cskylinetext`, s.`clongleadbody`,s.`cshortleadbody`,s.`cpubinfo`,s.`cdescribe`,s.`cbyline`,s.`eskylineheadline`, s.`eskylinetext`,s.`eheadline`,s.`elongleadbody`,s.`eshortleadbody`,s.`epubinfo`,s.`edescribe`, s.`ebyline`,s.`thumblink`,s.`piclink`,s.`rellink`,s.`genre`,s.`topic`,s.`industry`,s.`area`," +
                "s.`scoop`,s.`original`,s.`vcolumn`,s.`suppressad`,s.`adchannel`,s.`breadcrumb`,s.`customcolor`, s.`headlinesize`,s.`customlink`,s.`custombreadcrumb`,s.`showenglish`,s.`showpolling`,s.`accessright`, s.`methodeid`,s.`filename`,s.`videoname`,s.`status`,s.`pubdate`,s.`pubstatus`,s.`publish_status`, s.`adid`,s.`adbid`,s.`cc_vaddress`,s.`cc_piclink`,s.`captionid`,s.`caption`,s.`captionname` from video_story s ,(select id from channel_detail c where c.`chaid`=%d and c.`type`=2 limit %d,%d) b where s.id=b.id and s.publish_status='publish' order by s.pubdate desc;").format(_chanelId, (_pageIndex - 1) * _pageSize + 1, _pageSize)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                var tmpMap = Map[String, String]()
                tmpMap = tmpMap + ("id" -> rs.getString(1))
                tmpMap = tmpMap + ("cheadline" -> rs.getString(4))
                tmpMap = tmpMap + ("clongleadbody" -> rs.getString(7))
                tmpMap = tmpMap + ("cshortleadbody" -> rs.getString(8))
                tmpMap = tmpMap + ("column" -> rs.getString(29))
                tmpMap = tmpMap + ("pubdate" -> rs.getString(43))

                dataList = dataList :+ tmpMap
            }

            rs.close()
            ps.close()

            val tps = conn.prepareStatement("SELECT count(id) totalnum FROM channel_detail WHERE `type`=2 and chaid in (%d);".format(_chanelId))
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