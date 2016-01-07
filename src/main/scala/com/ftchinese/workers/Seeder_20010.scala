package com.ftchinese.workers

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.ftchinese.utils.Validators
import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * Get the target campaign data.
 * Created by wanbo on 2016/01/06.
 */
final class Seeder_20010 extends Seeder with ISeeder {

    name = "20010"

    driver = new MysqlDriver

    private var _code: String = ""
    private var _title: String = ""
    private var _tags: String = ""
    private var _type: String = ""
    private var _status: Int = 1
    private var _startDate: Long = 0
    private var _endDate: Long = 0
    private var _pageIndex: Int = 1
    private var _pageSize: Int = 10
    private var _pageTotal: Int = 0

    private val log = LoggerFactory.getLogger(classOf[Seeder_20010])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val code: String = seed.getOrElse("code", "").toString
            val title: String = seed.getOrElse("title", "").toString
            val tags: String = seed.getOrElse("tags", "").toString
            val cType: String = seed.getOrElse("type", "").toString

            if(code.length + title.length + tags.length + cType.length < 1)
                throw new EasyException("20001")

            if (code.length < 0 || code.length > 15)
                throw new EasyException("20001")
            if (title.length < 0 || title.length > 100)
                throw new EasyException("20001")
            if (tags.length < 0 || tags.length > 20)
                throw new EasyException("20001")
            if (cType.length < 0 || cType.length > 10)
                throw new EasyException("20001")

            _code = code
            _title = title
            _tags = tags

            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

            val startDate: String = seed.getOrElse("startdate", "").toString
            val endDate: String = seed.getOrElse("enddate", "").toString

            if(startDate.nonEmpty){
                if(Validators.check_date(startDate, "yyyy-MM-dd")){
                    _startDate = sdf.parse(startDate).getTime / 1000
                } else {
                    throw new EasyException("20001")
                }
            } else {
                _startDate = sdf.getCalendar.getTimeInMillis / 1000
            }

            if(endDate.nonEmpty){
                if(Validators.check_date(endDate, "yyyy-MM-dd")){
                    _endDate = sdf.parse(endDate).getTime / 1000
                } else {
                    throw new EasyException("20001")
                }
            } else {
                _endDate = sdf.getCalendar.getTimeInMillis / 1000
            }

            if(cType.nonEmpty){
                val types = cType.split(",").filter(_!="")
                if(types.size > 0)
                    _type = types.mkString(",")
            }

            val status: String = seed.getOrElse("status", "").toString

            if(status.nonEmpty){
                try{
                    val statusVal = status.toInt
                    if(statusVal > 1 && statusVal < 5)
                        _status = statusVal
                } catch {case e: Exception =>}
            }

            val pageIndex: String = seed.getOrElse("pageindex", "").toString

            if(pageIndex.nonEmpty){
                try{
                    val tmpVal = pageIndex.toInt
                    if(tmpVal > 1)
                        _pageIndex = tmpVal
                } catch {case e: Exception =>}
            }

            val pageSize: String = seed.getOrElse("pagesize", "").toString

            if(pageSize.nonEmpty){
                try{
                    val tmpVal = pageSize.toInt
                    if(tmpVal > 1)
                        _pageSize = tmpVal
                } catch {case e: Exception =>}
            }

            // Cache
            /*
            val cache_name = this.getClass.getSimpleName + _code

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
            cacher.close()*/

            dataList = onDBHandle()

            if (dataList.size < 1)
                throw new EasyException("20100")
            else {
                val cache_data = new EasyOutput
                cache_data.odata = dataList
            }

            if(_pageTotal > 0)
                fruits.oelement = fruits.oelement.updated("pagetotal", _pageTotal.toString)

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
            val conn = driver.getConnector("conferencedb")

            var where = Seq[String]()

            if(_status == 1)
                where :+= "con_isdeleted=0"
            else if (_status == 2)
                where :+= "(con_starttime < unix_timestamp() and con_endtime > unix_timestamp())"
            else if (_status == 3)
                where :+= "con_starttime > unix_timestamp()"
            else if (_status == 4)
                where :+= "con_endtime < unix_timestamp()"
            else {
                if(_startDate > 0 && _endDate > 0)
                    where :+= "((con_starttime < %d and con_endtime > %d) or (con_starttime < %d and con_endtime > %d))".format(_startDate, _startDate, _endDate, _endDate)
                else
                    where :+= "con_isdeleted=0"
            }

            if(_code.nonEmpty){
                where :+= " and con_code='%s'".format(_code)
            } else {
                if(_title.nonEmpty)
                    where :+= " and con_title like '%" + _title + "%'"

                if(_type.nonEmpty)
                    where :+= " and con_type in (%s)".format(_type)
            }

            if(_tags.nonEmpty){
                where :+= " and con_tag like '%," + _tags + ",%'"
            }

            where :+= " and con_type in (select tp_id from conference_type where tp_code < 9000) "

            val sql = "SELECT " +
                "con_code `code`, con_title `title`, con_type `type`, con_starttime `starttime`, con_endtime `endtime`, " +
                "con_link `link`, con_html `html`, con_tag `tag`, de_ccode `ccode`, de_image `image`, de_description `description`, " +
                "de_leader `leader`, de_organizer `organizer`, de_address `address`, de_award `award`, " +
                "de_awardnum `awardnum`, de_user1 `user1`, de_user2 `user2`, de_user3 `user3` " +
                "FROM conferencedb.conference_info left join conference_detail ON con_id = de_cid WHERE " +
                where.mkString + " order by starttime desc limit %d,%d;".format((_pageIndex - 1) * _pageSize, _pageSize)

            val sqlCount = "SELECT count(con_id) totalnum FROM conferencedb.conference_info WHERE " + where.mkString

            //log.info("SQL--------:" + sql)

            var ps = conn.prepareStatement(sql)
            var rs = ps.executeQuery()

            val metaData = ps.getMetaData
            val columnCount = metaData.getColumnCount

            while (rs.next()) {
                var tmpMap = Map[String, String]()
                for(i <- Range(1, columnCount + 1)) {
                    tmpMap = tmpMap + (metaData.getColumnLabel(i) -> rs.getString(i))
                }
                dataList = dataList :+ tmpMap
            }

            ps = conn.prepareStatement(sqlCount)
            rs = ps.executeQuery()

            if(rs.last()) {
                val pTotal = rs.getInt(1)
                if(pTotal > 0)
                    _pageTotal = pTotal
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