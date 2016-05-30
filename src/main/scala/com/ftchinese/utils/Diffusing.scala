package com.ftchinese.utils

/**
  * Diffusing algorithm
  * Created by wanbo on 16/5/30.
  */
class Diffusing {
    private var edges: List[Edge] = List()
    private var userVertex = Map[String, Double]()
    private var productVertex = Map[String, Double]()

    def fit(dataList: List[(String, String)]): Unit ={
        edges = dataList.map(x => Edge(x._1, x._2))
        userVertex = edges.map(x => (x.user, 0.0)).toMap
        productVertex = edges.map(x => (x.product, 0.0)).toMap
    }

    /**
      * Material diffusion
      *
      * @param user User
      * @return
      */
    def predictMD(user: String): List[(String, Double)] ={

        val targetUserProducts = edges.filter(_.user == user).map(_.product).toSet

        // Update products a default value
        productVertex.foreach(p => {
            if(targetUserProducts.contains(p._1)){
                productVertex = productVertex.updated(p._1, 1.0)
            }
        })

        // Update users value according products default value
        targetUserProducts.foreach(p => {
            val usersForProduct = edges.filter(_.product == p).map(_.user).toSet

            var uValue = 0.0
            val uSize = usersForProduct.size
            if(uSize > 0) {
                val pValue = productVertex(p)
                uValue = pValue / uSize

                userVertex = userVertex.map(u => {
                    if(usersForProduct.contains(u._1)){
                        (u._1, u._2 + uValue)
                    } else {
                        u
                    }
                })
            }
        })

        // Clean products value
        productVertex = cleanVertexValue(productVertex)

        // Update back to products by current users value
        userVertex.foreach{ case (user: String, value: Double) =>
                val productForUser = edges.filter(_.user == user).map(_.product).toSet

                var pValue = 0.0
                val pSize = productForUser.size

                if(pSize > 0){
                    pValue = value / pSize

                    productVertex = productVertex.map(p => {
                        if(productForUser.contains(p._1)) {
                            (p._1, p._2 + pValue)
                        } else {
                            p
                        }
                    })
                }
        }

        // Filter and predict valuable data
        val valuableData = productVertex.filter{case (product: String, value: Double) => !targetUserProducts.contains(product)}

        val predictSorted = valuableData.toList.sortBy(_._2)(Ordering.Double.reverse)

        val predictData = predictSorted.slice(0, 10)

        predictData
    }

    /**
      * Heat conduction
      *
      * @param user User
      * @return
      */
    def predictHC(user: String): List[(String, Double)] ={

        val targetUserProducts = edges.filter(_.user == user).map(_.product).toSet

        // Update products a default value
        productVertex.foreach(p => {
            if(targetUserProducts.contains(p._1)){
                productVertex = productVertex.updated(p._1, 1.0)
            }
        })

        // Update users value according products default value
        userVertex.foreach{ case (user: String, value: Double) =>
            val productForUser = edges.filter(_.user == user).map(_.product).toSet

            val pSize = productForUser.size
            if(pSize > 0) {
                val sumValues = productVertex.filter(p => productForUser.contains(p._1)).values.sum
                val uValue = sumValues / pSize
                userVertex = userVertex.updated(user, uValue)
            }

        }

        // Clean products value
        productVertex = cleanVertexValue(productVertex)

        // Update back to products by current users value
        productVertex.foreach{ case (product: String, value: Double) =>
            val userForProduct = edges.filter(_.product == product).map(_.user).toSet

            val uSize = userForProduct.size
            if(uSize > 0){
                val sumValues = userVertex.filter(u => userForProduct.contains(u._1)).values.sum
                val pValue = sumValues / uSize
                productVertex = productVertex.updated(product, pValue)
            }
        }

        // Filter and predict valuable data
        val valuableData = productVertex.filter{case (product: String, value: Double) => !targetUserProducts.contains(product)}

        val predictSorted = valuableData.toList.sortBy(_._2)(Ordering.Double.reverse)

        val predictData = predictSorted.slice(0, 10)

        predictData
    }

    private def cleanVertexValue(vertexData: Map[String, Double]): Map[String, Double] = vertexData.map(x => (x._1, 0.0))

    case class Edge(user: String = "", product: String = "")
}
