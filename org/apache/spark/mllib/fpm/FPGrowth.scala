/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.fpm

import java.{util => ju}
import java.lang.{Iterable => JavaIterable}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.{HashPartitioner, Logging, Partitioner, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.SparkContext._

/**
 * :: Experimental ::
 *
 * Model trained by [[FPGrowth]], which holds frequent itemsets.
 * @param freqItemsets frequent itemset, which is an RDD of (itemset, frequency) pairs
 * @tparam Item item type
 */
@Experimental
class FPGrowthModel[Item: ClassTag](
    val freqItemsets: RDD[(Array[Item], Long)]) extends Serializable {

  /** Returns frequent itemsets as a [[org.apache.spark.api.java.JavaPairRDD]]. */
  def javaFreqItemsets(): JavaPairRDD[Array[Item], java.lang.Long] = {
    JavaPairRDD.fromRDD(freqItemsets).asInstanceOf[JavaPairRDD[Array[Item], java.lang.Long]]
  }
}

/**
 * :: Experimental ::
 *
 * A parallel FP-growth algorithm to mine frequent itemsets. The algorithm is described in
 * [[http://dx.doi.org/10.1145/1454008.1454027 Li et al., PFP: Parallel FP-Growth for Query
 *  Recommendation]]. PFP distributes computation in such a way that each worker executes an
 * independent group of mining tasks. The FP-Growth algorithm is described in
 * [[http://dx.doi.org/10.1145/335191.335372 Han et al., Mining frequent patterns without candidate
 *  generation]].
 *
 * @param minSupport the minimal support level of the frequent pattern, any pattern appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 * @param numPartitions number of partitions used by parallel FP-growth
 *
 * @see [[http://en.wikipedia.org/wiki/Association_rule_learning Association rule learning
 *       (Wikipedia)]]
 */
@Experimental
class FPGrowth  (
    private var minSupport: Double,
    private var numPartitions: Int) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
   * as the input data}.
   */
  def this() = this(0.3, -1)

  /**
   * Sets the minimal support level (default: `0.3`).
   */
  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  
  /**
   * Sets the number of partitions used by parallel FP-growth (default: same as input data).
   */
  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  private var minCount= -1
  def setMinCount(mc:Int): this.type =
  {
    this.minCount=mc
    this
  }
  
  /**
   * Computes an FP-Growth model that contains frequent itemsets.
   * @param data input data set, each element contains a transaction
   * @return an [[FPGrowthModel]]
   */
  def run[Item: ClassTag](data: RDD[Array[Item]]): FPGrowthModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    
    val mc = if(minCount>0) minCount else 
    {
      val count = data.count()
      math.ceil(minSupport * count).toLong 
    }

    
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, mc, partitioner)
    val freqItemsets = genFreqItemsets(data, mc, freqItems, partitioner)
    freqItemsets.count()
    println("模型跑完了!")
    new FPGrowthModel(freqItemsets)
  }
  

  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPGrowthModel[Item] = {
    implicit val tag = fakeClassTag[Item]
    run(data.rdd.map(_.asScala.toArray))
  }

  /**
   * Generates frequent items by filtering the input data using minimal support level.
   * @param minCount minimum count for frequent itemsets
   * @param partitioner partitioner used to distribute items
   * @return array of frequent pattern ordered by their frequencies
   */
  private def genFreqItems[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      partitioner: Partitioner): Array[Item] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.size != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))//(item,1)
      .reduceByKey(partitioner, _ + _) //统计item的出现次数
      .filter(_._2 >= minCount) //过滤次数太少的
      .collect() //放到一起
      .sortBy(-_._2) //根据次数排序
      .map(_._1) //只要item,不要次数
  }

  /**
   * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
   * @param data transactions
   * @param minCount minimum count for frequent itemsets
   * @param freqItems frequent items
   * @param partitioner partitioner used to distribute transactions
   * @return an RDD of (frequent itemset, count)
   */
  private def genFreqItemsets[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      freqItems: Array[Item],
      partitioner: Partitioner): RDD[(Array[Item], Long)] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2)) //后缀相同的事务都落在同一棵树
    .flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }.map { case (ranks, count) =>
      (ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  /**
   * Generates conditional transactions.   根据一个事务,产生所有 条件事务
   * @param transaction a transaction
   * @param itemToRank map from item to their rank
   * @param partitioner partitioner used to distribute transactions
   * @return a map of (target partition, conditional transaction)
   */
  private def genCondTransactions[Item: ClassTag](
      transaction: Array[Item],
      itemToRank: Map[Item, Int],
      partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)//用rankId代替item
    ju.Arrays.sort(filtered)//排序
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item) //分配到一个partition,用的是Hash算法,一般情况,一个item一一对应一个partition
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}