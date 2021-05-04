package edu.uci.ics.texera.workflow.operators.sink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.immutable.List;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class SimpleSinkOpDesc extends OperatorDescriptor {

    // use SET_SNAPSHOT as the default output mode
    // this will be set internally by the workflow compiler
    @JsonIgnore
    private IncrementalOutputMode outputMode = IncrementalOutputMode.SET_SNAPSHOT;

    @Override
    public OpExecConfig operatorExecutor() {
        return new SimpleSinkOpExecConfig(this.operatorIdentifier(), outputMode);
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "View Results",
                "View the edu.uci.ics.texera.workflow results",
                OperatorGroupConstants.RESULT_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                List.empty());
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        return schemas[0];
    }

    @JsonIgnore
    public IncrementalOutputMode getOutputMode() {
        return outputMode;
    }

    @JsonIgnore
    public void setOutputMode(IncrementalOutputMode outputMode) {
        this.outputMode = outputMode;
    }

}
