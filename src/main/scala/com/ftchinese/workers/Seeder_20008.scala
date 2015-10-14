package com.ftchinese.workers

import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import com.wanbo.easyapi.shared.common.utils.Utils
import org.slf4j.LoggerFactory

/**
 * Submit campaign data.
 * Created by wanbo on 2015/9/10.
 */
final class Seeder_20008() extends Seeder with ISeeder {

    name = "20008"
    driver = new MysqlDriver

    private var _campaignId = ""

    private var _modList = List[(String, String)]()

    private val log = LoggerFactory.getLogger(classOf[Seeder_20008])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        try {

            val cId = seed.getOrElse("cid", "")

            if (cId != null)
                _campaignId = cId.toString


            if (_campaignId == "")
                throw new EasyException("20001")

            for(i <- Range(1, 18)){
                val tmpKey = "mod" + i
                _modList = _modList :+ (tmpKey, seed.getOrElse(tmpKey, "").toString)
            }

            val ret = onDBHandle()

            if(ret){
                fruits.oelement = fruits.oelement.updated("errorcode", "0")
            } else
                throw new EasyException("20100")

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

    override protected def onDBHandle(): Boolean = {
        var result = false

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector(dbName = "cmstmp01", writable = true)

            if (_campaignId == "")
                throw new EasyException("20100")

            // Whether the campaign exists.
            val sqlExists = "SELECT con_id totalnum FROM conferencedb.conference_info WHERE con_code='%s';".format(_campaignId)

            var ps = conn.prepareStatement(sqlExists)
            var rs = ps.executeQuery()

            if(!rs.next())
                throw new EasyException("20100")

            var keys = _modList.map(x => "con_" + x._1)
            var vals = _modList.map(_._2)

            keys = "con_cid" +: keys
            vals = _campaignId +: vals


            // To check whether the record exists.
            val checkKey = Utils.MD5(vals.mkString)
            val sqlCheck = "SELECT con_cid FROM conferencedb.conference_model where con_checkkey='%s';".format(checkKey)

            ps = conn.prepareStatement(sqlCheck)
            rs = ps.executeQuery()

            if(rs.next())
                throw new EasyException("20101")

            // End check

            keys = keys :+ "con_checkkey"
            vals = vals :+ checkKey
            val sqlAdd = "insert into conferencedb.conference_model (%s) values(%s);".format(keys.mkString(","), vals.mkString("'", "','", "'"))

            ps = conn.prepareStatement(sqlAdd)

            val addNum = ps.executeUpdate()

            if(addNum > 0)
                result = true
            else
                throw new Exception("Data write error.")

            rs.close()
            ps.close()
            conn.close()

        } catch {
            case e: Exception =>
                throw e
        }

        result
    }
}
