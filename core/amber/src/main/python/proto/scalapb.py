# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: scalapb/scalapb.proto
# plugin: python-betterproto
from dataclasses import dataclass
from typing import Dict, List

import betterproto

from .google import protobuf


class MatchType(betterproto.Enum):
    CONTAINS = 0
    EXACT = 1
    PRESENCE = 2


class ScalaPbOptionsOptionsScope(betterproto.Enum):
    FILE = 0
    PACKAGE = 1


class ScalaPbOptionsEnumValueNaming(betterproto.Enum):
    AS_IN_PROTO = 0
    CAMEL_CASE = 1


@dataclass
class ScalaPbOptions(betterproto.Message):
    # If set then it overrides the java_package and package.
    package_name: str = betterproto.string_field(1)
    # If true, the compiler does not append the proto base file name into the
    # generated package name. If false (the default), the generated scala package
    # name is the package_name.basename where basename is the proto file name
    # without the .proto extension.
    flat_package: bool = betterproto.bool_field(2)
    # Adds the following imports at the top of the file (this is meant to provide
    # implicit TypeMappers)
    import_: List[str] = betterproto.string_field(3)
    # Text to add to the generated scala file.  This can be used only when
    # single_file is true.
    preamble: List[str] = betterproto.string_field(4)
    # If true, all messages and enums (but not services) will be written to a
    # single Scala file.
    single_file: bool = betterproto.bool_field(5)
    # By default, wrappers defined at https://github.com/google/protobuf/blob/mas
    # ter/src/google/protobuf/wrappers.proto, are mapped to an Option[T] where T
    # is a primitive type. When this field is set to true, we do not perform this
    # transformation.
    no_primitive_wrappers: bool = betterproto.bool_field(7)
    # DEPRECATED. In ScalaPB <= 0.5.47, it was necessary to explicitly enable
    # primitive_wrappers. This field remains here for backwards compatibility,
    # but it has no effect on generated code. It is an error to set both
    # `primitive_wrappers` and `no_primitive_wrappers`.
    primitive_wrappers: bool = betterproto.bool_field(6)
    # Scala type to be used for repeated fields. If unspecified,
    # `scala.collection.Seq` will be used.
    collection_type: str = betterproto.string_field(8)
    # If set to true, all generated messages in this file will preserve unknown
    # fields.
    preserve_unknown_fields: bool = betterproto.bool_field(9)
    # If defined, sets the name of the file-level object that would be generated.
    # This object extends `GeneratedFileObject` and contains descriptors, and
    # list of message and enum companions.
    object_name: str = betterproto.string_field(10)
    # Experimental: scope to apply the given options.
    scope: "ScalaPbOptionsOptionsScope" = betterproto.enum_field(11)
    # If true, lenses will be generated.
    lenses: bool = betterproto.bool_field(12)
    # If true, then source-code info information will be included in the
    # generated code - normally the source code info is cleared out to reduce
    # code size.  The source code info is useful for extracting source code
    # location from the descriptors as well as comments.
    retain_source_code_info: bool = betterproto.bool_field(13)
    # Scala type to be used for maps. If unspecified,
    # `scala.collection.immutable.Map` will be used.
    map_type: str = betterproto.string_field(14)
    # If true, no default values will be generated in message constructors.
    no_default_values_in_constructor: bool = betterproto.bool_field(15)
    enum_value_naming: "ScalaPbOptionsEnumValueNaming" = betterproto.enum_field(16)
    # Indicate if prefix (enum name + optional underscore) should be removed in
    # scala code Strip is applied before enum value naming changes.
    enum_strip_prefix: bool = betterproto.bool_field(17)
    # Scala type to use for bytes fields.
    bytes_type: str = betterproto.string_field(21)
    # Enable java conversions for this file.
    java_conversions: bool = betterproto.bool_field(23)
    # List of message options to apply to some messages.
    aux_message_options: List[
        "ScalaPbOptionsAuxMessageOptions"
    ] = betterproto.message_field(18)
    # List of message options to apply to some fields.
    aux_field_options: List[
        "ScalaPbOptionsAuxFieldOptions"
    ] = betterproto.message_field(19)
    # List of message options to apply to some enums.
    aux_enum_options: List["ScalaPbOptionsAuxEnumOptions"] = betterproto.message_field(
        20
    )
    # List of enum value options to apply to some enum values.
    aux_enum_value_options: List[
        "ScalaPbOptionsAuxEnumValueOptions"
    ] = betterproto.message_field(22)
    # List of preprocessors to apply.
    preprocessors: List[str] = betterproto.string_field(24)
    field_transformations: List["FieldTransformation"] = betterproto.message_field(25)
    # Ignores all transformations for this file. This is meant to allow specific
    # files to opt out from transformations inherited through package-scoped
    # options.
    ignore_all_transformations: bool = betterproto.bool_field(26)
    # If true, getters will be generated.
    getters: bool = betterproto.bool_field(27)
    # For use in tests only. Inhibit Java conversions even when when generator
    # parameters request for it.
    test_only_no_java_conversions: bool = betterproto.bool_field(999)


@dataclass
class ScalaPbOptionsAuxMessageOptions(betterproto.Message):
    """
    AuxMessageOptions enables you to set message-level options through package-
    scoped options. This is useful when you can't add a dependency on
    scalapb.proto from the proto file that defines the message.
    """

    # The fully-qualified name of the message in the proto name space.
    target: str = betterproto.string_field(1)
    # Options to apply to the message. If there are any options defined on the
    # target message they take precedence over the options.
    options: "MessageOptions" = betterproto.message_field(2)


@dataclass
class ScalaPbOptionsAuxFieldOptions(betterproto.Message):
    """
    AuxFieldOptions enables you to set field-level options through package-
    scoped options. This is useful when you can't add a dependency on
    scalapb.proto from the proto file that defines the field.
    """

    # The fully-qualified name of the field in the proto name space.
    target: str = betterproto.string_field(1)
    # Options to apply to the field. If there are any options defined on the
    # target message they take precedence over the options.
    options: "FieldOptions" = betterproto.message_field(2)


@dataclass
class ScalaPbOptionsAuxEnumOptions(betterproto.Message):
    """
    AuxEnumOptions enables you to set enum-level options through package-scoped
    options. This is useful when you can't add a dependency on scalapb.proto
    from the proto file that defines the enum.
    """

    # The fully-qualified name of the enum in the proto name space.
    target: str = betterproto.string_field(1)
    # Options to apply to the enum. If there are any options defined on the
    # target enum they take precedence over the options.
    options: "EnumOptions" = betterproto.message_field(2)


@dataclass
class ScalaPbOptionsAuxEnumValueOptions(betterproto.Message):
    """
    AuxEnumValueOptions enables you to set enum value level options through
    package-scoped options.  This is useful when you can't add a dependency on
    scalapb.proto from the proto file that defines the enum.
    """

    # The fully-qualified name of the enum value in the proto name space.
    target: str = betterproto.string_field(1)
    # Options to apply to the enum value. If there are any options defined on the
    # target enum value they take precedence over the options.
    options: "EnumValueOptions" = betterproto.message_field(2)


@dataclass
class MessageOptions(betterproto.Message):
    # Additional classes and traits to mix in to the case class.
    extends: List[str] = betterproto.string_field(1)
    # Additional classes and traits to mix in to the companion object.
    companion_extends: List[str] = betterproto.string_field(2)
    # Custom annotations to add to the generated case class.
    annotations: List[str] = betterproto.string_field(3)
    # All instances of this message will be converted to this type. An implicit
    # TypeMapper must be present.
    type: str = betterproto.string_field(4)
    # Custom annotations to add to the companion object of the generated class.
    companion_annotations: List[str] = betterproto.string_field(5)
    # Additional classes and traits to mix in to generated sealed_oneof base
    # trait.
    sealed_oneof_extends: List[str] = betterproto.string_field(6)
    # If true, when this message is used as an optional field, do not wrap it in
    # an `Option`. This is equivalent of setting `(field).no_box` to true on each
    # field with the message type.
    no_box: bool = betterproto.bool_field(7)
    # Custom annotations to add to the generated `unknownFields` case class
    # field.
    unknown_fields_annotations: List[str] = betterproto.string_field(8)


@dataclass
class Collection(betterproto.Message):
    """
    Represents a custom Collection type in Scala. This allows ScalaPB to
    integrate with collection types that are different enough from the ones in
    the standard library.
    """

    # Type of the collection
    type: str = betterproto.string_field(1)
    # Set to true if this collection type is not allowed to be empty, for example
    # cats.data.NonEmptyList.  When true, ScalaPB will not generate `clearX` for
    # the repeated field and not provide a default argument in the constructor.
    non_empty: bool = betterproto.bool_field(2)
    # An Adapter is a Scala object available at runtime that provides certain
    # static methods that can operate on this collection type.
    adapter: str = betterproto.string_field(3)


@dataclass
class FieldOptions(betterproto.Message):
    type: str = betterproto.string_field(1)
    scala_name: str = betterproto.string_field(2)
    # Can be specified only if this field is repeated. If unspecified, it falls
    # back to the file option named `collection_type`, which defaults to
    # `scala.collection.Seq`.
    collection_type: str = betterproto.string_field(3)
    collection: "Collection" = betterproto.message_field(8)
    # If the field is a map, you can specify custom Scala types for the key or
    # value.
    key_type: str = betterproto.string_field(4)
    value_type: str = betterproto.string_field(5)
    # Custom annotations to add to the field.
    annotations: List[str] = betterproto.string_field(6)
    # Can be specified only if this field is a map. If unspecified, it falls back
    # to the file option named `map_type` which defaults to
    # `scala.collection.immutable.Map`
    map_type: str = betterproto.string_field(7)
    # Do not box this value in Option[T]. If set, this overrides
    # MessageOptions.no_box
    no_box: bool = betterproto.bool_field(30)
    # Like no_box it does not box a value in Option[T], but also fails parsing
    # when a value is not provided. This enables to emulate required fields in
    # proto3.
    required: bool = betterproto.bool_field(31)


@dataclass
class EnumOptions(betterproto.Message):
    # Additional classes and traits to mix in to the base trait
    extends: List[str] = betterproto.string_field(1)
    # Additional classes and traits to mix in to the companion object.
    companion_extends: List[str] = betterproto.string_field(2)
    # All instances of this enum will be converted to this type. An implicit
    # TypeMapper must be present.
    type: str = betterproto.string_field(3)
    # Custom annotations to add to the generated enum's base class.
    base_annotations: List[str] = betterproto.string_field(4)
    # Custom annotations to add to the generated trait.
    recognized_annotations: List[str] = betterproto.string_field(5)
    # Custom annotations to add to the generated Unrecognized case class.
    unrecognized_annotations: List[str] = betterproto.string_field(6)


@dataclass
class EnumValueOptions(betterproto.Message):
    # Additional classes and traits to mix in to an individual enum value.
    extends: List[str] = betterproto.string_field(1)
    # Name in Scala to use for this enum value.
    scala_name: str = betterproto.string_field(2)
    # Custom annotations to add to the generated case object for this enum value.
    annotations: List[str] = betterproto.string_field(3)


@dataclass
class OneofOptions(betterproto.Message):
    # Additional traits to mix in to a oneof.
    extends: List[str] = betterproto.string_field(1)
    # Name in Scala to use for this oneof field.
    scala_name: str = betterproto.string_field(2)


@dataclass
class FieldTransformation(betterproto.Message):
    when: protobuf.FieldDescriptorProto = betterproto.message_field(1)
    match_type: "MatchType" = betterproto.enum_field(2)
    set: protobuf.FieldOptions = betterproto.message_field(3)


@dataclass
class PreprocessorOutput(betterproto.Message):
    options_by_file: Dict[str, "ScalaPbOptions"] = betterproto.map_field(
        1, betterproto.TYPE_STRING, betterproto.TYPE_MESSAGE
    )
