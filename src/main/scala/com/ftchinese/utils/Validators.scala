package com.ftchinese.utils

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Validators
 * Created by wanbo on 16/1/6.
 */
object Validators {

    def check_email(emailStr: String): Boolean = {
        val r = """^[\w\-\.]+@[\w\-\.]+(\.\w+)+$""".r
        emailStr match {
            case r(x) =>
                true
            case _ =>
                false
        }
    }

    def check_date(dateStr: String, formatStr: String = "dd/MM/yyyy"): Boolean ={

        var date: Date = null

        try {
            val sdf = new SimpleDateFormat(formatStr)
            date = sdf.parse(dateStr)

            if(sdf.format(date) != dateStr)
                date = null

        } catch {
            case e: Exception =>
        }

        date != null
    }

}