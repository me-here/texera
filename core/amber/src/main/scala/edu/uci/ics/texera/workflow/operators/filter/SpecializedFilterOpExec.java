package edu.uci.ics.texera.workflow.operators.filter;

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.Function1;
import scala.Serializable;

import java.util.List;

public class SpecializedFilterOpExec extends FilterOpExec {

    private final SpecializedFilterOpDesc opDesc;

    public SpecializedFilterOpExec(SpecializedFilterOpDesc opDesc) {
        this.opDesc = opDesc;
        setFilterFunc(
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<Tuple, Boolean> & Serializable) this::filterFunc);
    }

    // MARK: Debug/unittests only
    public SpecializedFilterOpExec(SpecializedFilterOpDesc opDesc, List<FilterPredicate> predicates) {
        this(opDesc);
        opDesc.predicates = predicates;
    }

    public Boolean filterFunc(Tuple tuple) {
        boolean satisfy = false;
        for (FilterPredicate predicate : opDesc.predicates) {
            if (predicate.evaluate(tuple, opDesc.context())) {
                return true;
            }
        }
        return false;
    }

    ;

}
