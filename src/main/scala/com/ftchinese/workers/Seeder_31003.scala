package com.ftchinese.workers

import com.wanbo.easyapi.server.database.MysqlDriver
import com.wanbo.easyapi.server.lib.{EasyException, EasyOutput, ISeeder, Seeder}
import org.slf4j.LoggerFactory


/**
 * MyFT save configurations.
 * Created by wanbo on 2016/04/08.
 */
final class Seeder_31003 extends Seeder with ISeeder {

    name = "31003"

    driver = new MysqlDriver

    private var _uuId: String = ""
    private val _type: String = "myft"
    private var _name: String = ""
    private var _value: String = ""
    private var _cmd: Int = 1

    private val log = LoggerFactory.getLogger(classOf[Seeder_31003])

    override def onHandle(seed: Map[String, Any]): EasyOutput = {

        var dataList = List[Map[String, Any]]()

        try {
            val uuId: String = seed.getOrElse("uuid", "").toString
            val inType: String = seed.getOrElse("type", "").toString
            val inValue: String = seed.getOrElse("value", "").toString
            val cmd: String = seed.getOrElse("cmd", "").toString

            if(uuId.isEmpty)
                throw new EasyException("50002")

            if(uuId.length < 3 || uuId.length > 36)
                throw new EasyException("20001")

            if(inType.isEmpty || inValue.length > 40)
                throw new EasyException("20001")

            if(inType.isEmpty || inValue.length > 40)
                throw new EasyException("20001")

            if(cmd == "0")
                _cmd = 0

            _uuId = uuId.trim
            _name = inType.trim
            _value = inValue.trim


            // Write to database.
            dataList = onDBHandle()

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
        val dataList = List[Map[String, String]]()

        try {
            val driver = this.driver.asInstanceOf[MysqlDriver]
            val conn = driver.getConnector("user_db", writable = true)

            // Whether the campaign exists.
            var isExists = false
            var isFollowed = false
            val sqlExists = "SELECT `status` FROM `user_personal` WHERE `uuid`='%s' and `type`='%s' and `name`='%s' and `value`='%s' ;".format(_uuId, _type, _name, _value)

            var ps = conn.prepareStatement(sqlExists)
            val rs = ps.executeQuery()

            if(rs.next()) {
                isExists = true
                if(rs.getInt(1) == 1) isFollowed = true
            }

            var sql = ""
            if(_cmd == 1){
                if(isExists){

                    if(isFollowed)
                        throw new EasyException("20101")
                    else
                        sql = "UPDATE `user_personal` SET `status` = 1, last_update=current_timestamp WHERE `uuid`='%s' and `type`='%s' and `name`='%s' and `value`='%s';".format(_uuId, _type, _name, _value)

                } else {
                    sql = "INSERT `user_personal` (uuid, `type`, `name`, `value`) VALUES('%s', '%s', '%s', '%s');".format(_uuId, _type, _name, _value)
                }
            } else {
                if(isExists) {
                    sql = "UPDATE `user_personal` SET `status` = 0, last_update=current_timestamp WHERE `uuid`='%s' and `type`='%s' and `name`='%s' and `value`='%s';".format(_uuId, _type, _name, _value)
                } else {
                    throw new EasyException("20100")
                }
            }

            ps = conn.prepareStatement(sql)
            val num = ps.executeUpdate()

            if(num < 1){
                throw new Exception("Data write error.")
            }

            ps.close()

            conn.close()

        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }

}