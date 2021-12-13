// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package edu.uci.ics.texera.web.workflowruntimestate

object WorkflowruntimestateProto extends _root_.scalapb.GeneratedFileObject {
  lazy val dependencies: Seq[_root_.scalapb.GeneratedFileObject] = Seq(
    scalapb.options.ScalapbProto
  )
  lazy val messagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] =
    Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]](
      edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent,
      edu.uci.ics.texera.web.workflowruntimestate.OperatorRuntimeStats,
      edu.uci.ics.texera.web.workflowruntimestate.WorkflowJobRuntimeStats
    )
  private lazy val ProtoBytes: _root_.scala.Array[Byte] =
      scalapb.Encoding.fromBase64(scala.collection.immutable.Seq(
  """Ci1lZHUvdWNpL2ljcy90ZXhlcmEvd29ya2Zsb3dydW50aW1lc3RhdGUucHJvdG8SFmVkdS51Y2kuaWNzLnRleGVyYS53ZWIaF
  XNjYWxhcGIvc2NhbGFwYi5wcm90byLTAgoPQnJlYWtwb2ludEV2ZW50Ei0KCmFjdG9yX3BhdGgYASABKAlCDuI/CxIJYWN0b3JQY
  XRoUglhY3RvclBhdGgSWQoFdHVwbGUYAiABKAsyNy5lZHUudWNpLmljcy50ZXhlcmEud2ViLkJyZWFrcG9pbnRFdmVudC5CcmVha
  3BvaW50VHVwbGVCCuI/BxIFdHVwbGVSBXR1cGxlEjwKD2JyZWFrcG9pbnRfaW5mbxgDIAMoCUIT4j8QEg5icmVha3BvaW50SW5mb
  1IOYnJlYWtwb2ludEluZm8aeAoPQnJlYWtwb2ludFR1cGxlEhcKAmlkGAEgASgDQgfiPwQSAmlkUgJpZBInCghpc19pbnB1dBgCI
  AEoCEIM4j8JEgdpc0lucHV0Ugdpc0lucHV0EiMKBmZpZWxkcxgDIAMoCUIL4j8IEgZmaWVsZHNSBmZpZWxkcyLMAgoUT3BlcmF0b
  3JSdW50aW1lU3RhdHMSUQoFc3RhdGUYASABKA4yLy5lZHUudWNpLmljcy50ZXhlcmEud2ViLldvcmtmbG93QWdncmVnYXRlZFN0Y
  XRlQgriPwcSBXN0YXRlUgVzdGF0ZRIwCgtpbnB1dF9jb3VudBgCIAEoA0IP4j8MEgppbnB1dENvdW50UgppbnB1dENvdW50EjMKD
  G91dHB1dF9jb3VudBgDIAEoA0IQ4j8NEgtvdXRwdXRDb3VudFILb3V0cHV0Q291bnQSegoWdW5yZXNvbHZlZF9icmVha3BvaW50c
  xgEIAMoCzInLmVkdS51Y2kuaWNzLnRleGVyYS53ZWIuQnJlYWtwb2ludEV2ZW50QhriPxcSFXVucmVzb2x2ZWRCcmVha3BvaW50c
  1IVdW5yZXNvbHZlZEJyZWFrcG9pbnRzIvcBChdXb3JrZmxvd0pvYlJ1bnRpbWVTdGF0cxJRCgVzdGF0ZRgBIAEoDjIvLmVkdS51Y
  2kuaWNzLnRleGVyYS53ZWIuV29ya2Zsb3dBZ2dyZWdhdGVkU3RhdGVCCuI/BxIFc3RhdGVSBXN0YXRlEiAKBWVycm9yGAIgASgJQ
  griPwcSBWVycm9yUgVlcnJvchJnCg5vcGVyYXRvcl9zdGF0cxgDIAMoCzIsLmVkdS51Y2kuaWNzLnRleGVyYS53ZWIuT3BlcmF0b
  3JSdW50aW1lU3RhdHNCEuI/DxINb3BlcmF0b3JTdGF0c1INb3BlcmF0b3JTdGF0cyqKAQoXV29ya2Zsb3dBZ2dyZWdhdGVkU3Rhd
  GUSEQoNVU5JTklUSUFMSVpFRBAAEgkKBVJFQURZEAESCwoHUlVOTklORxACEgsKB1BBVVNJTkcQAxIKCgZQQVVTRUQQBBIMCghSR
  VNVTUlORxAFEg4KClJFQ09WRVJJTkcQBhINCglDT01QTEVURUQQB0IJ4j8GSABYAHgBYgZwcm90bzM="""
      ).mkString)
  lazy val scalaDescriptor: _root_.scalapb.descriptors.FileDescriptor = {
    val scalaProto = com.google.protobuf.descriptor.FileDescriptorProto.parseFrom(ProtoBytes)
    _root_.scalapb.descriptors.FileDescriptor.buildFrom(scalaProto, dependencies.map(_.scalaDescriptor))
  }
  lazy val javaDescriptor: com.google.protobuf.Descriptors.FileDescriptor = {
    val javaProto = com.google.protobuf.DescriptorProtos.FileDescriptorProto.parseFrom(ProtoBytes)
    com.google.protobuf.Descriptors.FileDescriptor.buildFrom(javaProto, _root_.scala.Array(
      scalapb.options.ScalapbProto.javaDescriptor
    ))
  }
  @deprecated("Use javaDescriptor instead. In a future version this will refer to scalaDescriptor.", "ScalaPB 0.5.47")
  def descriptor: com.google.protobuf.Descriptors.FileDescriptor = javaDescriptor
}