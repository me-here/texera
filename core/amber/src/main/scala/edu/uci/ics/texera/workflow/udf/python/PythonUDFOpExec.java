package edu.uci.ics.texera.workflow.udf.python;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.collection.Iterator;
import scala.util.Either;

public class PythonUDFOpExec implements OperatorExecutor {


    private final String code;


    public PythonUDFOpExec(String code) {
        this.code = code;
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }


    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        // Will not be used. The real implementation is in the Python UDF.
        return null;
    }

    public String getCode() {
        return code;
    }
}