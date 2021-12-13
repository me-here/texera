// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package edu.uci.ics.texera.web.workflowruntimestate

@SerialVersionUID(0L)
final case class BreakpointEvent(
    actorPath: _root_.scala.Predef.String,
    tuple: _root_.scala.Option[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple],
    breakpointInfo: _root_.scala.Seq[_root_.scala.Predef.String]
    ) extends scalapb.GeneratedMessage with scalapb.lenses.Updatable[BreakpointEvent] {
    @transient
    private[this] var __serializedSizeCachedValue: _root_.scala.Int = 0
    private[this] def __computeSerializedValue(): _root_.scala.Int = {
      var __size = 0
      
      {
        val __value = actorPath
        if (!__value.isEmpty) {
          __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(1, __value)
        }
      };
      if (tuple.isDefined) {
        val __value = tuple.get
        __size += 1 + _root_.com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(__value.serializedSize) + __value.serializedSize
      };
      breakpointInfo.foreach { __item =>
        val __value = __item
        __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(3, __value)
      }
      __size
    }
    override def serializedSize: _root_.scala.Int = {
      var read = __serializedSizeCachedValue
      if (read == 0) {
        read = __computeSerializedValue()
        __serializedSizeCachedValue = read
      }
      read
    }
    def writeTo(`_output__`: _root_.com.google.protobuf.CodedOutputStream): _root_.scala.Unit = {
      {
        val __v = actorPath
        if (!__v.isEmpty) {
          _output__.writeString(1, __v)
        }
      };
      tuple.foreach { __v =>
        val __m = __v
        _output__.writeTag(2, 2)
        _output__.writeUInt32NoTag(__m.serializedSize)
        __m.writeTo(_output__)
      };
      breakpointInfo.foreach { __v =>
        val __m = __v
        _output__.writeString(3, __m)
      };
    }
    def withActorPath(__v: _root_.scala.Predef.String): BreakpointEvent = copy(actorPath = __v)
    def getTuple: edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple = tuple.getOrElse(edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple.defaultInstance)
    def clearTuple: BreakpointEvent = copy(tuple = _root_.scala.None)
    def withTuple(__v: edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple): BreakpointEvent = copy(tuple = Option(__v))
    def clearBreakpointInfo = copy(breakpointInfo = _root_.scala.Seq.empty)
    def addBreakpointInfo(__vs: _root_.scala.Predef.String*): BreakpointEvent = addAllBreakpointInfo(__vs)
    def addAllBreakpointInfo(__vs: Iterable[_root_.scala.Predef.String]): BreakpointEvent = copy(breakpointInfo = breakpointInfo ++ __vs)
    def withBreakpointInfo(__v: _root_.scala.Seq[_root_.scala.Predef.String]): BreakpointEvent = copy(breakpointInfo = __v)
    def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
      (__fieldNumber: @_root_.scala.unchecked) match {
        case 1 => {
          val __t = actorPath
          if (__t != "") __t else null
        }
        case 2 => tuple.orNull
        case 3 => breakpointInfo
      }
    }
    def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
      _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
      (__field.number: @_root_.scala.unchecked) match {
        case 1 => _root_.scalapb.descriptors.PString(actorPath)
        case 2 => tuple.map(_.toPMessage).getOrElse(_root_.scalapb.descriptors.PEmpty)
        case 3 => _root_.scalapb.descriptors.PRepeated(breakpointInfo.iterator.map(_root_.scalapb.descriptors.PString(_)).toVector)
      }
    }
    def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToSingleLineUnicodeString(this)
    def companion = edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent
    // @@protoc_insertion_point(GeneratedMessage[edu.uci.ics.texera.web.BreakpointEvent])
}

object BreakpointEvent extends scalapb.GeneratedMessageCompanion[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent] {
  implicit def messageCompanion: scalapb.GeneratedMessageCompanion[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent] = this
  def parseFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent = {
    var __actorPath: _root_.scala.Predef.String = ""
    var __tuple: _root_.scala.Option[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple] = _root_.scala.None
    val __breakpointInfo: _root_.scala.collection.immutable.VectorBuilder[_root_.scala.Predef.String] = new _root_.scala.collection.immutable.VectorBuilder[_root_.scala.Predef.String]
    var _done__ = false
    while (!_done__) {
      val _tag__ = _input__.readTag()
      _tag__ match {
        case 0 => _done__ = true
        case 10 =>
          __actorPath = _input__.readStringRequireUtf8()
        case 18 =>
          __tuple = Option(__tuple.fold(_root_.scalapb.LiteParser.readMessage[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple](_input__))(_root_.scalapb.LiteParser.readMessage(_input__, _)))
        case 26 =>
          __breakpointInfo += _input__.readStringRequireUtf8()
        case tag => _input__.skipField(tag)
      }
    }
    edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent(
        actorPath = __actorPath,
        tuple = __tuple,
        breakpointInfo = __breakpointInfo.result()
    )
  }
  implicit def messageReads: _root_.scalapb.descriptors.Reads[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent] = _root_.scalapb.descriptors.Reads{
    case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
      _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage eq scalaDescriptor), "FieldDescriptor does not match message type.")
      edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent(
        actorPath = __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Predef.String]).getOrElse(""),
        tuple = __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).flatMap(_.as[_root_.scala.Option[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple]]),
        breakpointInfo = __fieldsMap.get(scalaDescriptor.findFieldByNumber(3).get).map(_.as[_root_.scala.Seq[_root_.scala.Predef.String]]).getOrElse(_root_.scala.Seq.empty)
      )
    case _ => throw new RuntimeException("Expected PMessage")
  }
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = WorkflowruntimestateProto.javaDescriptor.getMessageTypes().get(0)
  def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = WorkflowruntimestateProto.scalaDescriptor.messages(0)
  def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = {
    var __out: _root_.scalapb.GeneratedMessageCompanion[_] = null
    (__number: @_root_.scala.unchecked) match {
      case 2 => __out = edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple
    }
    __out
  }
  lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] =
    Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]](
      _root_.edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple
    )
  def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
  lazy val defaultInstance = edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent(
    actorPath = "",
    tuple = _root_.scala.None,
    breakpointInfo = _root_.scala.Seq.empty
  )
  @SerialVersionUID(0L)
  final case class BreakpointTuple(
      id: _root_.scala.Long,
      isInput: _root_.scala.Boolean,
      fields: _root_.scala.Seq[_root_.scala.Predef.String]
      ) extends scalapb.GeneratedMessage with scalapb.lenses.Updatable[BreakpointTuple] {
      @transient
      private[this] var __serializedSizeCachedValue: _root_.scala.Int = 0
      private[this] def __computeSerializedValue(): _root_.scala.Int = {
        var __size = 0
        
        {
          val __value = id
          if (__value != 0L) {
            __size += _root_.com.google.protobuf.CodedOutputStream.computeInt64Size(1, __value)
          }
        };
        
        {
          val __value = isInput
          if (__value != false) {
            __size += _root_.com.google.protobuf.CodedOutputStream.computeBoolSize(2, __value)
          }
        };
        fields.foreach { __item =>
          val __value = __item
          __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(3, __value)
        }
        __size
      }
      override def serializedSize: _root_.scala.Int = {
        var read = __serializedSizeCachedValue
        if (read == 0) {
          read = __computeSerializedValue()
          __serializedSizeCachedValue = read
        }
        read
      }
      def writeTo(`_output__`: _root_.com.google.protobuf.CodedOutputStream): _root_.scala.Unit = {
        {
          val __v = id
          if (__v != 0L) {
            _output__.writeInt64(1, __v)
          }
        };
        {
          val __v = isInput
          if (__v != false) {
            _output__.writeBool(2, __v)
          }
        };
        fields.foreach { __v =>
          val __m = __v
          _output__.writeString(3, __m)
        };
      }
      def withId(__v: _root_.scala.Long): BreakpointTuple = copy(id = __v)
      def withIsInput(__v: _root_.scala.Boolean): BreakpointTuple = copy(isInput = __v)
      def clearFields = copy(fields = _root_.scala.Seq.empty)
      def addFields(__vs: _root_.scala.Predef.String*): BreakpointTuple = addAllFields(__vs)
      def addAllFields(__vs: Iterable[_root_.scala.Predef.String]): BreakpointTuple = copy(fields = fields ++ __vs)
      def withFields(__v: _root_.scala.Seq[_root_.scala.Predef.String]): BreakpointTuple = copy(fields = __v)
      def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
        (__fieldNumber: @_root_.scala.unchecked) match {
          case 1 => {
            val __t = id
            if (__t != 0L) __t else null
          }
          case 2 => {
            val __t = isInput
            if (__t != false) __t else null
          }
          case 3 => fields
        }
      }
      def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
        _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
        (__field.number: @_root_.scala.unchecked) match {
          case 1 => _root_.scalapb.descriptors.PLong(id)
          case 2 => _root_.scalapb.descriptors.PBoolean(isInput)
          case 3 => _root_.scalapb.descriptors.PRepeated(fields.iterator.map(_root_.scalapb.descriptors.PString(_)).toVector)
        }
      }
      def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToSingleLineUnicodeString(this)
      def companion = edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple
      // @@protoc_insertion_point(GeneratedMessage[edu.uci.ics.texera.web.BreakpointEvent.BreakpointTuple])
  }
  
  object BreakpointTuple extends scalapb.GeneratedMessageCompanion[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple] {
    implicit def messageCompanion: scalapb.GeneratedMessageCompanion[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple] = this
    def parseFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple = {
      var __id: _root_.scala.Long = 0L
      var __isInput: _root_.scala.Boolean = false
      val __fields: _root_.scala.collection.immutable.VectorBuilder[_root_.scala.Predef.String] = new _root_.scala.collection.immutable.VectorBuilder[_root_.scala.Predef.String]
      var _done__ = false
      while (!_done__) {
        val _tag__ = _input__.readTag()
        _tag__ match {
          case 0 => _done__ = true
          case 8 =>
            __id = _input__.readInt64()
          case 16 =>
            __isInput = _input__.readBool()
          case 26 =>
            __fields += _input__.readStringRequireUtf8()
          case tag => _input__.skipField(tag)
        }
      }
      edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple(
          id = __id,
          isInput = __isInput,
          fields = __fields.result()
      )
    }
    implicit def messageReads: _root_.scalapb.descriptors.Reads[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple] = _root_.scalapb.descriptors.Reads{
      case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
        _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage eq scalaDescriptor), "FieldDescriptor does not match message type.")
        edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple(
          id = __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Long]).getOrElse(0L),
          isInput = __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).map(_.as[_root_.scala.Boolean]).getOrElse(false),
          fields = __fieldsMap.get(scalaDescriptor.findFieldByNumber(3).get).map(_.as[_root_.scala.Seq[_root_.scala.Predef.String]]).getOrElse(_root_.scala.Seq.empty)
        )
      case _ => throw new RuntimeException("Expected PMessage")
    }
    def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.javaDescriptor.getNestedTypes().get(0)
    def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.scalaDescriptor.nestedMessages(0)
    def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = throw new MatchError(__number)
    lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] = Seq.empty
    def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
    lazy val defaultInstance = edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple(
      id = 0L,
      isInput = false,
      fields = _root_.scala.Seq.empty
    )
    implicit class BreakpointTupleLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple](_l) {
      def id: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Long] = field(_.id)((c_, f_) => c_.copy(id = f_))
      def isInput: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Boolean] = field(_.isInput)((c_, f_) => c_.copy(isInput = f_))
      def fields: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[_root_.scala.Predef.String]] = field(_.fields)((c_, f_) => c_.copy(fields = f_))
    }
    final val ID_FIELD_NUMBER = 1
    final val IS_INPUT_FIELD_NUMBER = 2
    final val FIELDS_FIELD_NUMBER = 3
    def of(
      id: _root_.scala.Long,
      isInput: _root_.scala.Boolean,
      fields: _root_.scala.Seq[_root_.scala.Predef.String]
    ): _root_.edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple = _root_.edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple(
      id,
      isInput,
      fields
    )
    // @@protoc_insertion_point(GeneratedMessageCompanion[edu.uci.ics.texera.web.BreakpointEvent.BreakpointTuple])
  }
  
  implicit class BreakpointEventLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent](_l) {
    def actorPath: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Predef.String] = field(_.actorPath)((c_, f_) => c_.copy(actorPath = f_))
    def tuple: _root_.scalapb.lenses.Lens[UpperPB, edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple] = field(_.getTuple)((c_, f_) => c_.copy(tuple = Option(f_)))
    def optionalTuple: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Option[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple]] = field(_.tuple)((c_, f_) => c_.copy(tuple = f_))
    def breakpointInfo: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[_root_.scala.Predef.String]] = field(_.breakpointInfo)((c_, f_) => c_.copy(breakpointInfo = f_))
  }
  final val ACTOR_PATH_FIELD_NUMBER = 1
  final val TUPLE_FIELD_NUMBER = 2
  final val BREAKPOINT_INFO_FIELD_NUMBER = 3
  def of(
    actorPath: _root_.scala.Predef.String,
    tuple: _root_.scala.Option[edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple],
    breakpointInfo: _root_.scala.Seq[_root_.scala.Predef.String]
  ): _root_.edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent = _root_.edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent(
    actorPath,
    tuple,
    breakpointInfo
  )
  // @@protoc_insertion_point(GeneratedMessageCompanion[edu.uci.ics.texera.web.BreakpointEvent])
}
