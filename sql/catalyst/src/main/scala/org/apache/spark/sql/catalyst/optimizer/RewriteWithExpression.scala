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

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, PlanHelper, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, WITH_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrites the `With` expressions by adding a `Project` to pre-evaluate the common expressions, or
 * just inline them if they are cheap.
 *
 * Since this rule can introduce new `Project` operators, it is advised to run [[CollapseProject]]
 * after this rule.
 *
 * Note: For now we only use `With` in a few `RuntimeReplaceable` expressions. If we expand its
 *       usage, we should support aggregate/window functions as well.
 */
object RewriteWithExpression extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {

    val replaceMap: mutable.Map[Attribute, Attribute] = mutable.Map[Attribute, Attribute]()

    def replaceProjectAlias(p: Project): Project = {
      val newProjectList = p.projectList.map {
        case a: Alias if replaceMap.contains(a.toAttribute) =>
          val newChild = replaceMap(a.toAttribute)
          replaceMap.remove(a.toAttribute)
          a.copy(child = newChild)(
            exprId = a.exprId,
            qualifier = a.qualifier,
            explicitMetadata = a.explicitMetadata,
            nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
        case e => e
      }
      p.copy(projectList = newProjectList ++ replaceMap.values.toSeq)
    }

    def replaceAggregateAlias(agg: Aggregate): Aggregate = {
      val newGroupExprs = agg.groupingExpressions.toBuffer
      val newAggExprs = agg.aggregateExpressions.map {
        case a: Alias if replaceMap.contains(a.toAttribute) =>
          val newChild = replaceMap(a.toAttribute)
          newGroupExprs.append(newChild)
          replaceMap.remove(a.toAttribute)
          a.copy(child = newChild)(
            exprId = a.exprId,
            qualifier = a.qualifier,
            explicitMetadata = a.explicitMetadata,
            nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
        case e => e
      }
      agg.copy(groupingExpressions = newGroupExprs.toSeq ++ replaceMap.values.toSeq,
        aggregateExpressions = newAggExprs ++ replaceMap.values.toSeq)
    }

    plan.transformUpWithSubqueriesAndPruning(AlwaysProcess.fn) {
      // Common ExpressionDef with originAttribute from project and aggregate, we should replace
      // origin alias and make sure the attribute does not missing.
      case project: Project =>
        val newProject = replaceProjectAlias(project)
        if (newProject.expressions.exists(_.containsPattern(WITH_EXPRESSION))) {
          applyInternal(newProject, replaceMap)
        } else {
          newProject
        }
      case agg: Aggregate =>
        val newAgg = replaceAggregateAlias(agg)
        newAgg match {
          // For aggregates, separate the computation of the aggregations themselves from the final
          // result by moving the final result computation into a projection above it. This prevents
          // this rule from producing an invalid Aggregate operator.
          case p @ PhysicalAggregation(
          groupingExpressions, aggregateExpressions, resultExpressions, child)
            if p.expressions.exists(_.containsPattern(WITH_EXPRESSION)) =>
            // PhysicalAggregation returns aggregateExpressions as attribute references, which we
            // change to aliases so that they can be referred to by resultExpressions.
            val aggExprs = aggregateExpressions.map(
              ae => Alias(ae, "_aggregateexpression")(ae.resultId))
            val aggExprIds = aggExprs.map(_.exprId).toSet
            val resExprs = resultExpressions.map(_.transform {
              case a: AttributeReference if aggExprIds.contains(a.exprId) =>
                a.withName("_aggregateexpression")
            }.asInstanceOf[NamedExpression])
            // Rewrite the projection and the aggregate separately and then piece them together.
            val agg = Aggregate(groupingExpressions, groupingExpressions ++ aggExprs, child)
            val rewrittenAgg = applyInternal(agg, replaceMap)
            val proj = Project(resExprs, rewrittenAgg)
            applyInternal(proj, replaceMap)
          case _ => newAgg
        }
      case p if p.expressions.exists(_.containsPattern(WITH_EXPRESSION)) =>
        applyInternal(p, replaceMap)
    }
  }

  private def applyInternal(
      p: LogicalPlan,
      replaceMap: mutable.Map[Attribute, Attribute]): LogicalPlan = {
    var newPlan = p
    // We need completely bottom-up rewrite With, because With produced by push down predicate
    // can share the same CommonExpressionDef.
    while (newPlan.expressions.exists(_.containsPattern(WITH_EXPRESSION))) {
      val inputPlans = newPlan.children.toArray
      newPlan = newPlan.mapExpressions { expr =>
        rewriteWithExprAndInputPlans(expr, inputPlans, replaceMap)
      }
      val newInputPlans = inputPlans.map { input =>
        if (input.expressions.exists(_.containsPattern(WITH_EXPRESSION))) {
          applyInternal(input, replaceMap)
        } else {
          input
        }
      }
      newPlan = newPlan.withNewChildren(newInputPlans.toIndexedSeq)
    }
    // Since we add extra Projects with extra columns to pre-evaluate the common expressions,
    // the current operator may have extra columns if it inherits the output columns from its
    // child, and we need to project away the extra columns to keep the plan schema unchanged.
    assert(p.output.length <= newPlan.output.length)
    if (p.output.length + replaceMap.size < newPlan.output.length) {
      assert(p.outputSet.subsetOf(newPlan.outputSet))
      Project(p.output ++ replaceMap.values, newPlan)
    } else {
      newPlan
    }
  }

  private def rewriteWithExprAndInputPlans(
      e: Expression,
      inputPlans: Array[LogicalPlan],
      replaceMap: mutable.Map[Attribute, Attribute],
      isNestedWith: Boolean = false): Expression = {
    if (!e.containsPattern(WITH_EXPRESSION)) return e
    e match {
      // Do not handle nested With in one pass. Leave it to the next rule executor batch.
      case w: With if !isNestedWith =>
        // Rewrite nested With expressions first
        val child =
          rewriteWithExprAndInputPlans(w.child, inputPlans, replaceMap, isNestedWith = true)
        val defs =
          w.defs.map(rewriteWithExprAndInputPlans(_, inputPlans, replaceMap, isNestedWith = true))
        val refToExpr = mutable.HashMap.empty[CommonExpressionId, Expression]
        val childProjections = Array.fill(inputPlans.length)(mutable.ArrayBuffer.empty[Alias])

        defs.zipWithIndex.foreach { case (CommonExpressionDef(child, id, originAttribute), index) =>
          if (id.canonicalized) {
            throw SparkException.internalError(
              "Cannot rewrite canonicalized Common expression definitions")
          }

          if (CollapseProject.isCheap(child)) {
            refToExpr(id) = child
          } else {
            val childProjectionIndex = inputPlans.indexWhere(
              c => child.references.subsetOf(c.outputSet)
            )
            if (childProjectionIndex == -1) {
              // When we cannot rewrite the common expressions, force to inline them so that the
              // query can still run. This can happen if the join condition contains `With` and
              // the common expression references columns from both join sides.
              // TODO: things can go wrong if the common expression is nondeterministic. We
              //       don't fix it for now to match the old buggy behavior when certain
              //       `RuntimeReplaceable` did not use the `With` expression.
              // TODO: we should calculate the ref count and also inline the common expression
              //       if it's ref count is 1.
              refToExpr(id) = child
            } else if (originAttribute.nonEmpty && replaceMap.contains(originAttribute.get)) {
                refToExpr(id) = replaceMap(originAttribute.get)
            } else {
              val aliasName = if (SQLConf.get.getConf(SQLConf.USE_COMMON_EXPR_ID_FOR_ALIAS)) {
                s"_common_expr_${id.id}"
              } else {
                s"_common_expr_$index"
              }
              val alias = Alias(child, aliasName)()
              val fakeProj = Project(Seq(alias), inputPlans(childProjectionIndex))
              if (PlanHelper.specialExpressionsInUnsupportedOperator(fakeProj).nonEmpty) {
                // We have to inline the common expression if it cannot be put in a Project.
                refToExpr(id) = child
              } else {
                if (originAttribute.nonEmpty) {
                  replaceMap.put(originAttribute.get, alias.toAttribute)
                }
                childProjections(childProjectionIndex) += alias
                refToExpr(id) = alias.toAttribute
              }
            }
          }
        }

        for (i <- inputPlans.indices) {
          val projectList = childProjections(i)
          if (projectList.nonEmpty) {
            inputPlans(i) = Project(inputPlans(i).output ++ projectList, inputPlans(i))
          }
        }

        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          // `child` may contain nested With and we only replace `CommonExpressionRef` that
          // references common expressions in the current `With`.
          case ref: CommonExpressionRef if refToExpr.contains(ref.id) =>
            if (ref.id.canonicalized) {
              throw SparkException.internalError(
                "Cannot rewrite canonicalized Common expression references")
            }
            refToExpr(ref.id)
        }

      case c: ConditionalExpression =>
        val newAlwaysEvaluatedInputs = c.alwaysEvaluatedInputs.map(
          rewriteWithExprAndInputPlans(_, inputPlans, replaceMap, isNestedWith))
        val newExpr = c.withNewAlwaysEvaluatedInputs(newAlwaysEvaluatedInputs)
        // Use transformUp to handle nested With.
        newExpr.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
          case With(child, defs) =>
            // For With in the conditional branches, they may not be evaluated at all and we can't
            // pull the common expressions into a project which will always be evaluated. Inline it.
            val refToExpr = defs.map(d => d.id -> d.child).toMap
            child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
              case ref: CommonExpressionRef => refToExpr(ref.id)
            }
        }

      case other =>
        other.mapChildren(rewriteWithExprAndInputPlans(_, inputPlans, replaceMap, isNestedWith))
    }
  }
}
