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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * FP-Tree data structure used in FP-Growth.
 * @tparam T item type
 */
private[fpm] class FPTree[T] extends Serializable {

  import FPTree._

  val root: Node[T] = new Node(null)

  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty
  //应该是某个item在fpTree的对应的各个节点

  /** Adds a transaction with count. */  /*对fpTree插入一个 事务*/
  def add(t: Iterable[T], count: Long = 1L): this.type = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item => //对于事务t中的每个item
      //找到该item对应的summary,已存在则get,不存在则创建新的
      val summary = summaries.getOrElseUpdate(item, new Summary)
      //该item的出现的总次数加1
      summary.count += count
      //从跟节点开始,一步一步往下走
      //如果下一目标节点已存在则直接修改,否则创建新的节点插入再修改
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        summary.nodes += newNode
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: FPTree[T]): this.type = {
    other.transactions.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  /** Gets a subtree with the suffix. */
  /*根据一个后缀,获得以这个后缀节点为叶子节点的子树,这里得到的就是conditinal fp-tree*/
  private def project(suffix: T): FPTree[T] = {
    val tree = new FPTree[T]
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach { node => //遍历该item对应的每个节点
        var t = List.empty[T]
        var curr = node.parent
        while (!curr.isRoot) { //对于每个节点,往上找父节点,组成一个事务
          t = curr.item :: t
          curr = curr.parent
        }
        tree.add(t, node.count)//通过插入事务的形式建立树
      }
    }
    tree
  }

  /** Returns all transactions in an iterator. */
  def transactions: Iterator[(List[T], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  /*本质上,FpTree就是 事务集合的压缩表示.该方法是从FpTree还原出所有事务*/
  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(
      minCount: Long,
      validateSuffix: T => Boolean = _ => true): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>//对当前树所包含的每个item
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>        //递归抽取在这里
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }
}

private[fpm] object FPTree {

  /** Representing a node in an FP-Tree. */
  class Node[T](val parent: Node[T]) extends Serializable {
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null
  }

  /** Summary of a item in an FP-Tree. */
  private class Summary[T] extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }
}