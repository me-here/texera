package edu.uci.ics.texera.workflow.operators.pythonUDF;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.tuple.ITuple;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.collection.Iterator;
import scala.util.Either;

public class PythonUDFOpExec implements OperatorExecutor {


    private final String code;


    PythonUDFOpExec(String code) {
        this.code = code;
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }

    @Override
    public String getParam(String query) {
        return OperatorExecutor.super.getParam(query);
    }


    @Override
    public Iterator<ITuple> processTuple(Either<ITuple, InputExhausted> tuple, LinkIdentity input) {
        return OperatorExecutor.super.processTuple(tuple, input);
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        return null;
    }

    public String getCode() {
        return code;
    }
}
