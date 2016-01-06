package com.ftchinese

import com.ftchinese.utils.Validators

/**
 * Test
 * Created by wanbo on 16/1/6.
 */
object Test_Validator {
    def main(args: Array[String]) {
        val s = "11/31/2016"


        println(Validators.check_date(s))
    }
}
