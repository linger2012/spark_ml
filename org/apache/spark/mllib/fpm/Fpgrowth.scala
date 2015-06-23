package org.apache.spark.mllib.fpm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable.Map
import scala.tools.ant.sabbus.Break

object FP_Growth {

  def main(args: Array[String]) = {
    val support_percent = 0.3
    val pnum = 32
    val conf = new SparkConf().set("spark.executor.memory", "8G").setAppName("Fpg")
    val sc = new SparkContext(conf)
    val file = sc.textFile(args(0))
    val line_num = file.count()
    
    //remove the repeat line
    //line key:transcationID 
    //line value:itemIDs
    val f = file.map(line => (
      line.split(",")
      .drop(1)
      .toList
      .sortWith(_ < _), 1))
      .reduceByKey(_ + _)
    
    
    //compute support rate of every item
    //line key:itemIDs
    //line value:the number of line appear
    //g's element key:itemID
    //g's element value:support rate of every item
    val g = f.flatMap(line => {
      var l = List[(String, Int)]()
      for (i <- line._1) {
        l = (i, line._2) :: l
      }
      l
    })
      .reduceByKey(_ + _)
      .sortBy(_._2, false) //disascend sort
      .cache() //persist this RDD with the default storage level

    //convert g to a array
    val g_list = g.collect.toArray

    //the number of g_list' item
    var g_count = 0
    //convert g_list to a map
    //g_map's element key : itemID
    //g_map's element value : serial number
    val g_map = Map[String, Int]()

    //number for the itemID
    for (i <- g_list) {
      g_count = g_count + 1
      g_map(i._1) = g_count
    }

    //compute the number of serial items
    //t key:itemIDs
    //t value:the number of line appear
    //item's element key : the serial numbers of item
    //item's element value : the number of item appear
    val item = f.map(t => {
      var l = List[Int]()
      for (i <- t._1)
        l = g_map(i) :: l
      (l.sortWith(_ < _), t._2)
    })

    //======================================
    //compute the min_support rating
    val support_num: Int = item.map(t => (1, t._2))
      .reduceByKey(_ + _)
      .first()._2 * support_percent toInt

    val g_size = (g_count + pnum - 1) / pnum
    //divide items into groups
    //t key : the serial numbers of item
    //t value : the number of item appear
    //f_list's element key : groupID
    //f_list's element value : the prefix items and corresponding number of the group
    val f_list = item.flatMap(t => {
      var pre = -1
      var i = t._1.length - 1
      var result = List[(Int, (List[Int], Int))]()
      while (i >= 0) {
        if ((t._1(i) - 1) / g_size != pre) {
          pre = (t._1(i) - 1) / g_size
          result = (pre, (t._1.dropRight(t._1.length - i - 1), t._2)) :: result
        }
        i -= 1
      }
      result
    })
      .groupByKey()
      .cache()

    //parallelize FP-Growth
    //t key : groupID
    //t value : the prefix items and corresponding number of the group
    //d_result's element key : frequent itemset
    //d_result's element value : the number of frequent itemset appear
    val d_result = f_list.flatMap(t => {
      fp_growth(t._2, support_num, t._1 * g_size + 1 to (((t._1 + 1) * g_size) min g_count))
    })

    //save result into required format  
    val temp_result = d_result.map(t => (t._1.map(a => g_list(a - 1)._1), t._2))
    val result = temp_result.map( t => ( listToString(t._1)._2, listToString(t._1)._1 + ":" + t._2.toFloat/line_num.toFloat ) ).groupBy(_._1)
   
    //result.map(t => t._2.map(s => s._2)).saveAsTextFile(args(1))
    println("等待模型跑完")
    println("count:"+result.count())

    sc.stop()
  }
  
  //convert list to string
  def listToString(l: List[String]): (String, Int) = {
    var str = ""
    var count = 0
    for (i <- l) {
      str += i + ","
      count += 1
    }
    str = "\r\n" + str.substring(0, str.size - 1)
    return (str, count)
  }
  /*
  * fp-tree' growth  
  * contains make tree,prefix cut,deal with target,deal with single branche and mining frequent itemset
  */                             
  def fp_growth(v: Iterable[(List[Int], Int)], min_support: Int, target: Iterable[Int] = null): List[(List[Int], Int)] = {
    val root = new tree(null, null, 0)
    val tab = Map[Int, tree]()
    val tabc = Map[Int, Int]()
    //make tree
    for (i <- v) {
      var cur = root;
      var s: tree = null
      var list = i._1
      while (!list.isEmpty) {
        if (!tab.exists(_._1 == list(0))) {
          tab(list(0)) = null
        }
        if (!cur.son.exists(_._1 == list(0))) {
          s = new tree(cur, tab(list(0)), list(0))
          tab(list(0)) = s
          cur.son(list(0)) = s
        } else {
          s = cur.son(list(0))
        }
        s.support += i._2
        cur = s
        list = list.drop(1)

      }
    }
    //prefix cut
    for (i <- tab.keys) {
      var count = 0
      var cur = tab(i)
      while (cur != null) {
        count += cur.support
        cur = cur.Gnext
      }
      //modify
      tabc(i) = count
      if (count < min_support) {
        var cur = tab(i)
        while (cur != null) {
          var s = cur.Gnext
          cur.Gparent.son.remove(cur.Gv)
          cur = s
        }
        tab.remove(i)
      }
    }
    //deal with target
    var r = List[(List[Int], Int)]()
    var tail: Iterable[Int] = null
    if (target == null)
      tail = tab.keys
    else {
      tail = target.filter(a => tab.exists(b => b._1 == a))
    }
    if (tail.count(t => true) == 0)
      return r
    //deal with the single branch
    var cur = root
    var c = 1
    while (c < 2) {
      c = cur.son.count(t => true)
      if (c == 0) {
        var res = List[(Int, Int)]()
        while (cur != root) {
          res = (cur.Gv, cur.support) :: res
          cur = cur.Gparent
        }

        val part = res.partition(t1 => tail.exists(t2 => t1._1 == t2))
        val p1 = gen(part._1)
        if (part._2.length == 0)
          return p1
        else
          return decare(p1, gen(part._2)) ::: p1
      }
      cur = cur.son.values.head
    }
    //mining the frequent itemset
    for (i <- tail) {
      var result = List[(List[Int], Int)]()
      var cur = tab(i)
      while (cur != null) {
        var item = List[Int]()
        var s = cur.Gparent
        while (s != root) {
          item = s.Gv :: item
          s = s.Gparent
        }
        result = (item, cur.support) :: result
        cur = cur.Gnext
      }
      r = (List(i), tabc(i)) :: fp_growth(result, min_support).map(t => (i :: t._1, t._2)) ::: r

    }
    r
  }

  def gen(tab: List[(Int, Int)]): List[(List[Int], Int)] = {
    if (tab.length == 1) {
      return List((List(tab(0)._1), tab(0)._2))
    }
    val sp = tab(0)
    val t = gen(tab.drop(1))
    return (List(sp._1), sp._2) :: t.map(s => (sp._1 :: s._1, s._2 min sp._2)) ::: t
    //TODO: sp._2 may not be min
  }

  //笛卡尔积
  def decare[T](a: List[(List[T], Int)], b: List[(List[T], Int)]): List[(List[T], Int)] = {
    var res = List[(List[T], Int)]()
    for (i <- a)
      for (j <- b)
        res = (i._1 ::: j._1, i._2 min j._2) :: res
    res
  }
}

class tree(parent: tree, next: tree, v: Int) {
  val son = Map[Int, tree]()
  var support = 0
  def Gparent = parent
  def Gv = v
  def Gnext = next
}