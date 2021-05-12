package edu.uci.ics.amber.engine.common
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

class FusedOperatorExecutor(operators:IOperatorExecutor*) extends IOperatorExecutor {

  private val inputCounter = Array.fill(operators.length)(0L)
  private val outputCounter = Array.fill(operators.length)(0L)

  class LazyTupleIteratorOuter(upstream:Iterator[Either[ITuple, InputExhausted]], iterGen:Either[ITuple, InputExhausted] => Iterator[ITuple], outputCallback:() => Unit) extends Iterator[ITuple]{
    private var iter:Iterator[ITuple] = Iterator.empty

    override def hasNext: Boolean = {
      while(!iter.hasNext && upstream.hasNext) {
        val elem = upstream.next()
        iter = iterGen(elem)
      }
      iter.hasNext
    }

    override def next(): ITuple = {
      outputCallback()
        iter.next()
    }
  }

  class LazyTupleIteratorInner(upstream:Iterator[Either[ITuple, InputExhausted]], iterGen:Either[ITuple, InputExhausted] => Iterator[ITuple]) extends Iterator[Either[ITuple, InputExhausted]]{

    private var iter:Iterator[Either[ITuple, InputExhausted]] = Iterator.empty

    override def hasNext: Boolean ={
      while(!iter.hasNext && upstream.hasNext){
        val elem = upstream.next()
        iter = iterGen(elem).filter(_!=null).map(x => Left(x))
        if(elem.isRight)iter ++= Iterator(elem)
      }
      iter.hasNext
    }

    override def next(): Either[ITuple, InputExhausted] = {
     iter.next()
    }
  }


  override def open(): Unit = {
    operators.foreach(_.open())
  }

  override def close(): Unit = {
    operators.foreach(_.close())
  }

  override def processTuple(tuple: Either[ITuple, InputExhausted], input: LinkIdentity): Iterator[ITuple] = {
    var curIter = Iterator(tuple)
    val end = operators.length - 1
    for(i <- 0 until end){
      curIter = new LazyTupleIteratorInner(curIter, t => {
        if(i>0){
          outputCounter(i-1)+=1
        }
        inputCounter(i)+=1
        operators(i).processTuple(t, input)
      })
    }
    new LazyTupleIteratorOuter(curIter, t =>{
      outputCounter(end-1)+=1
      inputCounter(end)+=1
      operators(end).processTuple(t, input)
    }, () => outputCounter(end)+=1)
  }

  def getOperatorStatistics:Array[(Long,Long)] = {
    inputCounter.zip(outputCounter)
  }
}
