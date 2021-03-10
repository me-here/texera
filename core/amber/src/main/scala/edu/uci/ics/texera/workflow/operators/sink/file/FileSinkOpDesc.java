package edu.uci.ics.texera.workflow.operators.sink.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;
import edu.uci.ics.texera.workflow.common.operators.sink.SinkOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.jooq.types.UInteger;
import scala.collection.immutable.List;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class FileSinkOpDesc extends SinkOpDesc {

    @JsonProperty(required = true)
    @JsonSchemaTitle("file name")
    @JsonPropertyDescription("The file name to save to workflow results")
    public String fileName;

    @JsonProperty(required = true)
    @JsonSchemaTitle("file type")
    @JsonPropertyDescription("The type to save the workflow results to")
    public ResultFileType fileType;

    @Override
    public OpExecConfig operatorExecutor() {
        // Current file saving is using user id as identifier so user is is required.
//        if (context().userID().isEmpty()){
//            throw new RuntimeException("User id is null");
//        }
//        return new FileSinkOpExecConfig(this.operatorIdentifier(), fileName, fileType, context().userID().get());
        return new FileSinkOpExecConfig(this.operatorIdentifier(), fileName, fileType, UInteger.valueOf("12345"));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Save To File",
                "save the edu.uci.ics.texera.workflow results into file",
                OperatorGroupConstants.RESULT_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                List.empty());
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        return schemas[0];
    }

}
