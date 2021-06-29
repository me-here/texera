package edu.uci.ics.amber.engine.common.virtualidentity

object util {

  lazy val CONTROLLER: ControllerVirtualIdentity = ControllerVirtualIdentity("controller")
  lazy val SELF: SelfVirtualIdentity = SelfVirtualIdentity("self")
  lazy val CLIENT: ClientVirtualIdentity = ClientVirtualIdentity("client")

  def makeLayer(operatorIdentity: OperatorIdentity, layerID: String): LayerIdentity = {
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, layerID)
  }

  def toOperatorIdentity(layerIdentity: LayerIdentity): OperatorIdentity =
    OperatorIdentity(layerIdentity.workflow, layerIdentity.operator)
}
