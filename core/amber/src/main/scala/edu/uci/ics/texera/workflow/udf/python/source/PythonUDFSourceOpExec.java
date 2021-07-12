package edu.uci.ics.texera.workflow.udf.python.source;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.tuple.ITuple;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.udf.python.PythonUDFOpExec;
import scala.collection.Iterator;
import scala.util.Either;

public class PythonUDFSourceOpExec extends PythonUDFOpExec implements SourceOperatorExecutor {


    public PythonUDFSourceOpExec(String code) {
        super(code);
    }


    @Override
    public Iterator<ITuple> processTuple(Either<ITuple, InputExhausted> tuple, LinkIdentity input) {
        return SourceOperatorExecutor.super.processTuple(tuple, input);
        // Will not be used. The real implementation is in the Python UDF.
    }

    @Override
    public Iterator<ITuple> produce() {
        // Will not be used. The real implementation is in the Python UDF.
        return SourceOperatorExecutor.super.produce();
    }

    @Override
    public Iterator<Tuple> produceTexeraTuple() {
        // Will not be used. The real implementation is in the Python UDF.
        return null;
    }
}