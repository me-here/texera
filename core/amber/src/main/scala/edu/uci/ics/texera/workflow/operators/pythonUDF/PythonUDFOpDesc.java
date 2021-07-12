package edu.uci.ics.texera.workflow.operators.pythonUDF;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.engine.common.IOperatorExecutor;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.ManyToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Function1;

import java.util.List;

import static edu.uci.ics.texera.workflow.operators.pythonUDF.PythonUDFType.SupervisedTraining;
import static edu.uci.ics.texera.workflow.operators.pythonUDF.PythonUDFType.UnsupervisedTraining;
import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class PythonUDFOpDesc extends OperatorDescriptor {

    @JsonProperty()
    @JsonSchemaTitle("Python script")
    @JsonPropertyDescription("input your code here")
    public String pythonScriptText;


    @JsonProperty(required = true)
    @JsonSchemaTitle("pythonUDFType")
    public PythonUDFType pythonUDFType;

    @JsonProperty()
    @JsonSchemaTitle("Extra output column(s)")
    @JsonPropertyDescription("name of the newly added output columns that the UDF will produce, if any")
    public List<Attribute> outputColumns;


    @Override
    public OpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        Function1<Object, IOperatorExecutor> exec = (i) ->
                new PythonUDFOpExec(pythonScriptText);
        if (PythonUDFType.supportsParallel.contains(pythonUDFType)) {
            return new OneToOneOpExecConfig(operatorIdentifier(), exec);
        } else {
            // changed it to 1 because training with python needs all data in one node.
            return new ManyToOneOpExecConfig(operatorIdentifier(), exec);
        }
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Python UDF",
                "User-defined function operator in Python script",
                OperatorGroupConstants.UDF_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Schema inputSchema = schemas[0];


        Schema.Builder outputSchemaBuilder = Schema.newBuilder();
        if (pythonUDFType == SupervisedTraining) {
            outputSchemaBuilder.add("class", AttributeType.STRING);
            outputSchemaBuilder.add("precision", AttributeType.STRING);
            outputSchemaBuilder.add("recall", AttributeType.STRING);
            outputSchemaBuilder.add("f1-score", AttributeType.STRING);
            outputSchemaBuilder.add("support", AttributeType.STRING);
        } else if (pythonUDFType == UnsupervisedTraining) {
            outputSchemaBuilder.add("output", AttributeType.STRING);
        } else {
            // for pythonUDFType with map and filter, keep the same schema from input
            outputSchemaBuilder.add(inputSchema);
        }

        // for any pythonUDFType, it can add custom output columns (attributes).
        if (outputColumns != null) {
            for (Attribute column : outputColumns) {
                if (inputSchema.containsAttribute(column.getName()))
                    throw new RuntimeException("Column name " + column.getName()
                            + " already exists!");
            }
            outputSchemaBuilder.add(outputColumns).build();
        }
        return outputSchemaBuilder.build();
    }
}
