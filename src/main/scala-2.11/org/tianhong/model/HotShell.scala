package org.tianhong.model

/**
  * Created by lta on 2017/3/3.
  */

/*
* ProductHotID
productID
productName
productSpec
productPrice
productURL
downloadTime
productImage

* */
case class HotShell(
                    // val rowKey: String,
                     val productHotSellID: String, //主鍵對外关联ID
                     val productID: String, //自身ID
                     val productName: String,
                     val productSpec: String,
                     val productPrice: Double,
                     val productURL: String,
                     val downloadTime: String,
                     val productImage: String,
                     val platformID: Int,
                     val sparkID: String
                   ) extends Serializable

object HotShellField {
  val productID: String = "productID"
  val productHotSellID: String = "productHotSellID"

  val productName: String = "productName"
  val productSpec: String = "productSpec"
  val productPrice: String = "productPrice"
  val productURL: String = "productURL"
  val downloadTime: String = "downloadTime"
  val productImage: String = "productImage"
  val uniqueness: String = "uniqueness"
}
