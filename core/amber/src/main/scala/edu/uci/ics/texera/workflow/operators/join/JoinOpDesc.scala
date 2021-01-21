package edu.uci.ics.texera.workflow.operators.join

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameOnPort1}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class JoinOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("left attribute")
  @JsonPropertyDescription("left join key")
  @AutofillAttributeName
  var leftAttr: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("right attribute")
  @JsonPropertyDescription("right join key")
  @AutofillAttributeNameOnPort1
  var rightAttr: String = _

  override def operatorExecutor: OpExecConfig = {
    // TODO: Implement this
    throw new NotImplementedError()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("left"), InputPort("right")),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    // TODO: Implement this
    null
  }

}
