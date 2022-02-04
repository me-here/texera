package edu.uci.ics.texera.workflow.operators.specializedFilter

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.filter.{ComparisonType, FilterPredicate, SpecializedFilterOpDesc, SpecializedFilterOpExec}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Arrays.asList

class SpecializedFilterOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  var counter: Int = 0

  val tupleSchema: Schema = Schema
    .newBuilder()
    .add(new Attribute("string", AttributeType.STRING))
    .add(new Attribute("int", AttributeType.INTEGER))
    .add(new Attribute("bool", AttributeType.BOOLEAN))
    .add(new Attribute("long", AttributeType.LONG))
    .build()

  def layerID(): LayerIdentity = {
    counter += 1
    LayerIdentity("" + counter, "" + counter, "" + counter)
  }

  def linkID(): LinkIdentity = LinkIdentity(layerID(), layerID())

  def markerValue(): String = {
    "RandomValue"
  }

  def allNullTuple(): Tuple = {
    counter += 1
    Tuple
      .newBuilder(tupleSchema)
      .add(new Attribute("string", AttributeType.STRING), null)
      .add(new Attribute("int", AttributeType.INTEGER), null)
      .add(new Attribute("bool", AttributeType.BOOLEAN), null)
      .add(new Attribute("long", AttributeType.LONG), null)
      .build()
  }

  def nonNullTuple(): Tuple = {
    counter += 1
    Tuple
      .newBuilder(tupleSchema)
      .add(new Attribute("string", AttributeType.STRING), "hello")
      .add(new Attribute("int", AttributeType.INTEGER), 0)
      .add(new Attribute("bool", AttributeType.BOOLEAN), false)
      .add(new Attribute("long", AttributeType.LONG), Long.MaxValue)
      .build()
  }

  before {
    counter = 0
  }

  it should "open and close" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc())
    opExec.open()
    opExec.close()
  }

  it should "include all tuples from 1 completely null input stream of string type for is null" in {
    val predicates = asList(
      new FilterPredicate("string", ComparisonType.IS_NULL, null),
    )
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc(), predicates)
    opExec.open()

    val range = Range(0, 7)
    val linkID1 = linkID()

    val resultSize = range
      .map(_ => {
        val result = opExec.processTexeraTuple(Left(allNullTuple()), linkID1)
        val empty = Iterator()
        if (result == empty) 0 else 1
      })
      .reduce((count1, count2) => count1 + count2)

    assert(resultSize == range.size)

    opExec.close()
  }

  it should "include no tuples from 1 null input stream of string type for is not null" in {
    val predicates = asList(new FilterPredicate("string", ComparisonType.IS_NOT_NULL, null))
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc(), predicates)
    opExec.open()

    val range = Range(0, 7)
    val linkID1 = linkID()

    val resultSize = range
      .map(_ => {
        val result = opExec.processTexeraTuple(Left(allNullTuple()), linkID1)
        val empty = Iterator()
        if (result == empty) 0 else 1
      })
      .reduce((count1, count2) => count1 + count2)

    assert(resultSize == 0)
    opExec.close()
  }

  it should "throw NullPointerException when predicates is null" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc())
    opExec.open()
    val linkID1 = linkID()

    assertThrows[NullPointerException] {
      Range(0, 4).map(i => {
        opExec.processTexeraTuple(Left(allNullTuple()), linkID1)
      })
    }

    opExec.close()
  }



  it should "do nothing when predicates is an empty list" in {
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc(), asList())
    opExec.open()
    val linkID1 = linkID()

    val range = Range(0, 4)
    val resultSize = range
      .map(_ => {
        val result = opExec.processTexeraTuple(Left(allNullTuple()), linkID1)
        val empty = Iterator()
        if (result == empty) 0 else 1
      })
      .reduce((count1, count2) => count1 + count2)

    assert(resultSize == 0)
    opExec.close()
  }

  it should "not have null comparisons be affected by values, only comparison types" in {
    val predicates = asList(new FilterPredicate("string", ComparisonType.IS_NULL, markerValue()))
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc(), predicates)
    opExec.open()

    val range = Range(0, 7)
    val linkID1 = linkID()

    val resultSize = range
      .map(_ => {
        val result = opExec.processTexeraTuple(Left(allNullTuple()), linkID1)
        val empty = Iterator()
        if (result == empty) 0 else 1
      })
      .reduce((count1, count2) => count1 + count2)

    assert(resultSize == range.size)
    opExec.close()
  }

  it should "filter the right values with 2 input streams with is null" in {
    val predicates = asList(
      new FilterPredicate("bool", ComparisonType.IS_NULL, null),
    )
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc(), predicates)
    opExec.open()

    val inclusionRange = Range(0, 7)
    val exclusionRange = Range(0, 9)
    val linkID1 = linkID()
    val linkID2 = linkID()
    var countIncluded = 0

    inclusionRange.map(_ => {
      val result = opExec.processTexeraTuple(Left(allNullTuple()), linkID1)
      countIncluded = if (result == Iterator()) countIncluded else countIncluded+1
      result
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID1).isEmpty)

    exclusionRange.map(_ => {
      val result = opExec.processTexeraTuple(Left(nonNullTuple()), linkID2)
      countIncluded = if (result == Iterator()) countIncluded else countIncluded+1
      result
    })

    assert(countIncluded == inclusionRange.size)

    opExec.close()
  }

  it should "filter the right values with 2 input streams with is not null" in {
    val predicates = asList(new FilterPredicate("bool", ComparisonType.IS_NOT_NULL, null))
    val opExec = new SpecializedFilterOpExec(new SpecializedFilterOpDesc(), predicates)
    opExec.open()

    val linkID1 = linkID()
    val linkID2 = linkID()
    var countIncluded = 0

    val inclusionRange = Range(0, 7)
    val exclusionRange = Range(0, 9)

    exclusionRange.map(_ => {
      val result = opExec.processTexeraTuple(Left(allNullTuple()), linkID1)
      countIncluded = if (result == Iterator()) countIncluded else countIncluded+1
      result
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), linkID1).isEmpty)

    inclusionRange.map(_ => {
      val result = opExec.processTexeraTuple(Left(nonNullTuple()), linkID2)
      countIncluded = if (result == Iterator()) countIncluded else countIncluded+1
      result
    })

    assert(countIncluded == inclusionRange.size)
    opExec.close()
  }
}
