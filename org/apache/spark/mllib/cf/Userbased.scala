package org.apache.spark.mllib.cf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.SparkContext._ 
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.SparseVector
import scala.collection.mutable.ArrayBuffer

class Userbased (itemUserMatrix: IndexedRowMatrix)
{
 
  def recommend(output:String)=
  {
     if(userSimilarity==null) similarity
     
     val itemUsers = itemUserMatrix.rows.map { row => (row.index,row.vector) }
     val allUsers = userSimilarity.map(row=>row._1)
        
     //产生需要预测的 user,item对
     val userItemAndOthers = allUsers.cartesian(itemUsers).filter{case (user,(item,users))=> users(user.toInt) ==null || users(user.toInt)==0.0}    
     val userItemPair = userItemAndOthers.map(row=>(row._1,row._2._1.toInt))
    
     val predictions = predict(userItemPair).map(row=>(row._1._1,(row._1._2,row._2)))
     
     val userItems = predictions.aggregateByKey(new ArrayBuffer[(Int, Double)],888)((U,V)=>U+=V, (U1,U2)=>U1++=U2)
     //注意,聚合完之后是无序的
     val res = userItems.map{case (user,itemAndScores)=>
       val sorted =  itemAndScores.sortBy(row=>row._2)
       user+":"+sorted.reverse.take(100).mkString(",")
     }
     res.saveAsTextFile(output)
       
  }
   
  var userSimilarity: RDD[(Long, Vector)]=null
  def similarity = 
  {
    //用户相似度计算
    val userSimUpper = itemUserMatrix.toRowMatrix().columnSimilarities()
    val userSimFull = userSimUpper.entries.flatMap { entry=>
      if(entry.value != 0.0)
      {
        val array = new Array[MatrixEntry](2)  
        array(0)=new MatrixEntry(entry.i,entry.j,entry.value*0.5+0.5)
        array(1)=new MatrixEntry(entry.j,entry.i,entry.value*0.5+0.5)
        array
      }
      else null //矩阵本来就是不会存放0.0,所以这里应该不会执行
    } 
    val userSim = new CoordinateMatrix(userSimFull).toIndexedRowMatrix()
    userSimilarity = userSim.rows.map{row=>(row.index,row.vector)}
  }
  
  def predict(toPredictPair: RDD[(Long, Int)]):RDD[((Long, Int), Double)]=
  {
    
    if(userSimilarity==null) similarity
      
    val itemUsers = itemUserMatrix.rows.map { row => (row.index,row.vector) }
    
    val userKey= toPredictPair.join(userSimilarity) //(user,(item,usersim))
    val itemKey = userKey.map(row=>(row._2._1.toLong,(row._1,row._2._2)))
    val itemAndUserAndOther = itemKey.join(itemUsers)  // (item,((user,usersim),itemUser))
    
    val predictions = itemAndUserAndOther.map{row=>
      val item = row._1
      val user = row._2._1._1
      val userSim = row._2._1._2//有些user不一定用到
      val itemUsers = row._2._2//对该item打过分的user
      
      var sum=0.0
      var nu=0.0
      itemUsers.foreachActive{(user,score)=>
        val sim = userSim(user)
        nu+=sim*score
        sum+=sim
        }     
      ((user,item.toInt),nu/sum)
    }
    predictions
  }
   
  
  def test(ratings: RDD[(Long, Int, Double)])=
  {
  
    //预测分数
    val toPredictPair = ratings.map(rating=>(rating._1,rating._2))   
    val predictions = predict(toPredictPair)
    
    //计算Rmse
     val pairs = predictions.join(ratings.map(f=>((f._1,f._2),f._3))).values
     math.sqrt(pairs.map(x => (x._1 - x._2) * (x._1 - x._2)).mean()) 
    
    
  }
  
}






object UserbasedApp
{
  def recDemo(trainFile:String,output:String)=
  {     
       val conf = new SparkConf().setAppName("Userbased Recomendation")  
       conf.set("spark.executor.memory", "16G").set("spark.cores.max","88").set("spark.driver.maxResultSize","3G")
       
       
       //Job aborted due to stage failure: Total size of serialized results of 4 tasks (1054.0 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
       //http://www.sxt.cn/u/4647/blog/5467 Shuffle Map Task运算结果的处理
       val sc = new SparkContext(conf)   
       
       val trainData = new RatingInput(sc,trainFile,88,true) //分区大小影响速度  
       
       
       val model = new Userbased(trainData.get)     
       model.recommend(output)   
       
       sc.stop()
  }
    
  def rmseDemo(trainFile:String,testFile:String)=
  {   
       val conf = new SparkConf().setAppName("Userbased Recomendation")  
       conf.set("spark.executor.memory", "16G").set("spark.cores.max","88")
       val sc = new SparkContext(conf) 
       
       val trainData = new RatingInput(sc,trainFile,true)     
       val model = new Userbased(trainData.get)  
        
        val testData =sc.textFile(testFile)
        val ratings = testData.map{ line => 
          val fields = line.split(',')
          (fields(0).toLong,fields(1).toInt,fields(2).toDouble)      
       }
       
       val rmse = model.test(ratings)
       println("rmse:"+rmse)
       sc.stop()
  }
  
  
  def main(args : Array[String]) =
  {
      val flag = args(0)
      if(flag.equals("rec"))
      {
        val trainFile= args(1)
        val output = args(2)
        recDemo(trainFile,output)
      }
      else if(flag.equals("test"))
      {
         val trainFile= args(1)
         val testFile=  args(2) 
         rmseDemo(trainFile,testFile)
      }
      else 
      {
        println("error!")
      }
      
  }

}