package edu.uci.ics.texera.workflow.udf.python;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
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
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Function1;

import java.util.List;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class PythonUDFOpDesc extends OperatorDescriptor {

    @JsonProperty(required = true)
    @JsonSchemaTitle("Python script")
    @JsonPropertyDescription("Input your code here")
    public String code;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Parallel")
    @JsonPropertyDescription("Run with multiple workers?")
    public Boolean parallel;

    @JsonProperty()
    @JsonSchemaTitle("Extra output column(s)")
    @JsonPropertyDescription("Name of the newly added output columns that the UDF will produce, if any")
    public List<Attribute> outputColumns;


    @Override
    public OpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        Function1<Object, IOperatorExecutor> exec = (i) ->
                new PythonUDFOpExec(code);
        if (parallel) {
            return new OneToOneOpExecConfig(operatorIdentifier(), exec);
        } else {
            return new ManyToOneOpExecConfig(operatorIdentifier(), exec);
        }

    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Python UDF",
                "User-defined function operator in Python script",
                OperatorGroupConstants.UDF_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", true))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        Schema inputSchema = schemas[0];

        Schema.Builder outputSchemaBuilder = Schema.newBuilder();

        // keep the same schema from input
        outputSchemaBuilder.add(inputSchema);

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