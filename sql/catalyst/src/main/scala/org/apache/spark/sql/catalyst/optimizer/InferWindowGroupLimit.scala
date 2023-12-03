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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentRow, DenseRank, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, Literal, NamedExpression, PredicateHelper, Rank, RowNumber, SizeBasedWindowFunction, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, LIMIT, WINDOW}

/**
 * Inserts a `WindowGroupLimit` below `Window` if the `Window` has rank-like functions
 * and the function results are further filtered by limit-like predicates or cumulative
 * aggregation with limit excludes `SizeBasedWindowFunction`. Example query:
 * {{{
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE rn = 5
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE 5 = rn
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE rn < 5
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE 5 > rn
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE rn <= 5
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE 5 >= rn
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 limit 5
 *   SELECT *, sum(b) OVER(PARTITION BY k ORDER BY a) AS s FROM Tab1 limit 5
 * }}}
 */
object InferWindowGroupLimit extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Extract all the limit values from predicates.
   */
  def extractLimits(condition: Expression, attr: Attribute): Option[Int] = {
    val limits = splitConjunctivePredicates(condition).collect {
      case EqualTo(IntegerLiteral(limit), e) if e.semanticEquals(attr) => limit
      case EqualTo(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => limit
      case LessThan(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => limit - 1
      case GreaterThan(IntegerLiteral(limit), e) if e.semanticEquals(attr) => limit - 1
      case LessThanOrEqual(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => limit
      case GreaterThanOrEqual(IntegerLiteral(limit), e) if e.semanticEquals(attr) => limit
    }

    if (limits.nonEmpty) Some(limits.min) else None
  }

  /**
   * All window expressions should use the same expanding window, so that
   * we can safely do the early stop.
   */
  private def isExpandingWindow(
      windowExpression: NamedExpression): Boolean = windowExpression match {
    case Alias(WindowExpression(windowFunction, WindowSpecDefinition(_, _,
        SpecifiedWindowFrame(_, UnboundedPreceding, CurrentRow))), _)
      if !windowFunction.isInstanceOf[SizeBasedWindowFunction] => true
    case _ => false
  }

  private def support(windowFunction: Expression): Boolean = windowFunction match {
    case _: Rank | _: DenseRank | _: RowNumber => true
    case _ => false
  }

  private def limitSupport(limit: Int, window: Window): Boolean =
    limit <= conf.windowGroupLimitThreshold && window.child.maxRows.forall(_ > limit) &&
      !window.child.isInstanceOf[WindowGroupLimit] &&
      window.windowExpressions.forall(isExpandingWindow) &&
      window.orderSpec.exists(!_.foldable) &&
      // LimitPushDownThroughWindow have better performance than WindowGroupLimit if the
      // window function is Rank, DenseRank and RowNumber, and Window partitionSpec is empty.
      !(window.partitionSpec.isEmpty && supportsPushdownThroughWindow(window.windowExpressions) &&
        limit < conf.topKSortFallbackThreshold)

  private def supportsPushdownThroughWindow(
      windowExpressions: Seq[NamedExpression]): Boolean = windowExpressions.forall {
    case Alias(WindowExpression(_: Rank | _: DenseRank | _: RowNumber, WindowSpecDefinition(Nil, _,
      SpecifiedWindowFrame(_, UnboundedPreceding, CurrentRow))), _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.windowGroupLimitThreshold == -1) return plan

    plan.transformWithPruning(
      t => t.containsPattern(WINDOW) && t.containsAnyPattern(FILTER, LIMIT), ruleId) {
      case localLimit @ LocalLimit(IntegerLiteral(limit),
        window @ Window(_, partitionSpec, orderSpec, child)) if limitSupport(limit, window) =>
        // RankLimitIterator is selected through rank() to ensure that enough rows are obtained.
        val windowGroupLimit = WindowGroupLimit(partitionSpec, orderSpec, new Rank(), limit, child)
        localLimit.withNewChildren(Seq(window.withNewChildren(Seq(windowGroupLimit))))
      case localLimit @ LocalLimit(IntegerLiteral(limit), project @ Project(_,
        window @ Window(_, partitionSpec, orderSpec, child))) if limitSupport(limit, window) =>
        val windowGroupLimit = WindowGroupLimit(partitionSpec, orderSpec, new Rank(), limit, child)
        localLimit.withNewChildren(Seq(
          project.withNewChildren(Seq(window.withNewChildren(Seq(windowGroupLimit))))))
      case filter @ Filter(condition,
        window @ Window(windowExpressions, partitionSpec, orderSpec, child))
        if !child.isInstanceOf[WindowGroupLimit] && windowExpressions.forall(isExpandingWindow) &&
          orderSpec.nonEmpty =>
        val limits = windowExpressions.collect {
          case alias @ Alias(WindowExpression(rankLikeFunction, _), _)
            if support(rankLikeFunction) =>
            extractLimits(condition, alias.toAttribute).map((_, rankLikeFunction))
        }.flatten

        if (limits.isEmpty) {
          filter
        } else {
          val (rowNumberLimits, otherLimits) = limits.partition(_._2.isInstanceOf[RowNumber])
          // Pick RowNumber first as it's cheaper to evaluate.
          val selectedLimits = if (rowNumberLimits.isEmpty) {
            otherLimits
          } else {
            rowNumberLimits
          }
          // Pick a rank-like function with the smallest limit
          selectedLimits.minBy(_._1) match {
            case (limit, rankLikeFunction) if limit <= conf.windowGroupLimitThreshold &&
              child.maxRows.forall(_ > limit) =>
              if (limit > 0) {
                val newFilterChild = if (rankLikeFunction.isInstanceOf[RowNumber] &&
                  partitionSpec.isEmpty && limit < conf.topKSortFallbackThreshold) {
                  // Top n (Limit + Sort) have better performance than WindowGroupLimit if the
                  // window function is RowNumber and Window partitionSpec is empty.
                  Limit(Literal(limit), window)
                } else {
                  val windowGroupLimit =
                    WindowGroupLimit(partitionSpec, orderSpec, rankLikeFunction, limit, child)
                  window.withNewChildren(Seq(windowGroupLimit))
                }
                filter.withNewChildren(Seq(newFilterChild))
              } else {
                LocalRelation(filter.output, data = Seq.empty, isStreaming = filter.isStreaming)
              }
            case _ =>
              filter
          }
        }
    }
  }
}
