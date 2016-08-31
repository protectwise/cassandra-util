package com.protectwise

/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package object cql {

  def In[T](values: Iterable[T]): ArgumentPlaceholder = {
    if (values.isEmpty) NoArg
    else VarArgs(values).asInstanceOf[ArgumentPlaceholder] // just to shut up Idea's internal compiler which is spooked by this for some reason
  }

  def Fields(values: Iterable[(String, Any)]): ArgumentPlaceholder = {
    if (values.isEmpty) NoArg
    else KeyValArgs(values).asInstanceOf[ArgumentPlaceholder]
  }

  def NonEmptyFields(values: Iterable[(String, Any)]): ArgumentPlaceholder = {
    if (values.isEmpty) NoArg
    else KeyValArgs(values).nonEmpty.asInstanceOf[ArgumentPlaceholder]
  }

  implicit class CQLHelper(val sc: StringContext) extends AnyVal {

    def cql(args: Any*): CQLStatement = {
      val strings = sc.parts.iterator.toSeq
      val expressions = args.iterator.toSeq map CQLStatement.convert

      val namedParameters: Map[Symbol, Seq[Int]] =
        expressions
          .zipWithIndex
          .filter(_._1.isInstanceOf[Symbol])
          .groupBy(_._1.asInstanceOf[Symbol])
          .mapValues(_.map(_._2))

      CQLStatement(strings, expressions, namedParameters)
    }
  }

}

package cql {

  sealed trait ArgumentPlaceholder

  /**
    * An In() operator helper - this lets you pass a collection to a statement and
    * generate a series of bound parameters to represent values in the collection.
    * NOTE that this should be used cautiously with prepared statements, because
    * each count of objects for the In() operator will generate a *different* cql
    * statement.
    *
    * @param values
    * @tparam T
    */
  case class VarArgs[T <: Any](values: Iterable[T], separator: String = ",") extends ArgumentPlaceholder

  case class KeyValArgs(args: Iterable[(String, Any)], kvSeparator: String = "=", pairSeparator: String = ",") extends ArgumentPlaceholder {
    def nonEmpty = copy(args.filter {
      case (k, None) => false
      case (k, null) => false
      case _ => true
    })
    def keys: ArgumentPlaceholder = Inline(args.map(_._1).mkString(pairSeparator)).asInstanceOf[ArgumentPlaceholder]
    def values: ArgumentPlaceholder = In(args.map(_._2))
  }

  /**
    * Inline text pseudo-arg for adding non-parameterized text to a CQL statement
    *
    * @param text
    */
  case class Inline(text: String) extends ArgumentPlaceholder

  /**
    * Placeholder to indicate that a bound position doesn't represent an actual
    * bound parameter position
    */
  object NoArg extends Inline("")

}

