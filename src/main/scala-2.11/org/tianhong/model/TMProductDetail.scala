package org.tianhong.model

/**
  * Created by lta on 2017/3/2.
  */

case class ProductDetailModel(
                                 val productID: String, // 商品ID
                                 val platformID: Int,
                                 val productName: String, //商品名称
                                 val productBrand: String, //品牌
                                 val productSpec: String, //规格
                                 val productPrice: Double, //价格
                                 val commentCount: Int, //评论数
                                 val enshrineCount: Int, //收藏数目
                                 val weeklySales: Int, //  周销量
                                 val monthlySales: Int, //月销量
                                 val salesEvents: String, //促销活动
                                 val category: String, // 类目
                                 val standardCategory: String, // 精确类目
                                 val downloadTime: String, //下载时间
                                 val productHotSellID: String, //爆品的关联ID
                                 val productArea: String,
                                 val fitPeople: String,
                                 val productCode: String,
                                 val categoryPath:String,
                                 val index:Int,
                                 val isWorldWide:String,
                                 val groupBuying:String,
                                 val sparkID: String
                               ) extends Serializable

//case class ProductDetailModel(
//                                 val productID: String, // 商品ID
//                                 val productName: String, //商品名称
//                                 val productBrand: String, //品牌
//                                 val productSpec: String, //规格
//                                 val productPrice: String, //价格
//                                 val commentCount: String, //评论数
//                                 val enshrineCount: String, //收藏数目
//                                 val weeklySales:String ,//  周销量
//                                 val monthlySales: String, //月销量
//                                 val salesEvents: String, //促销活动
//                                 val category: String, // 类目
//                                 val standardCategory: String, // 精确类目
//                                 val downloadTime: String, //下载时间
//                                 val hotSellID: String //爆品的关联ID
//                               ) extends  Serializable

object ProductDetailGetField {
  val platformID: String = "platformID"
  val productID: String = "productID"
  val productName: String = "productName"
  val productBrand: String = "productBrand"
  val productSpec: String = "productSpec"
  val productPrice: String = "productPrice"
  val commentCount: String = "commentCount"
  val enshrineCount: String = "enshrineCount"
  val weeklySales: String = "weeklySales"
  val monthlySales: String = "monthlySales"
  val salesEvents: String = "salesEvents"
  val category: String = "category"
  val standardCategory: String = "standardCategory"
  val downloadTime: String = "downloadTime"
  val productHotSellID: String = "hotSellID"
}