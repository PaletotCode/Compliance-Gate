from __future__ import annotations

from enum import StrEnum


class LiteralValueType(StrEnum):
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    DATE = "date"
    NULL = "null"


class ExpressionDataType(StrEnum):
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    DATE = "date"
    NULL = "null"
    LIST_STRING = "list[string]"
    LIST_INT = "list[int]"
    LIST_FLOAT = "list[float]"
    LIST_BOOL = "list[bool]"
    LIST_DATE = "list[date]"
    UNKNOWN = "unknown"


_TYPE_ALIASES: dict[str, ExpressionDataType] = {
    "string": ExpressionDataType.STRING,
    "str": ExpressionDataType.STRING,
    "utf8": ExpressionDataType.STRING,
    "large_string": ExpressionDataType.STRING,
    "varchar": ExpressionDataType.STRING,
    "text": ExpressionDataType.STRING,
    "int": ExpressionDataType.INT,
    "int8": ExpressionDataType.INT,
    "int16": ExpressionDataType.INT,
    "int32": ExpressionDataType.INT,
    "int64": ExpressionDataType.INT,
    "i8": ExpressionDataType.INT,
    "i16": ExpressionDataType.INT,
    "i32": ExpressionDataType.INT,
    "i64": ExpressionDataType.INT,
    "uint8": ExpressionDataType.INT,
    "uint16": ExpressionDataType.INT,
    "uint32": ExpressionDataType.INT,
    "uint64": ExpressionDataType.INT,
    "u8": ExpressionDataType.INT,
    "u16": ExpressionDataType.INT,
    "u32": ExpressionDataType.INT,
    "u64": ExpressionDataType.INT,
    "float": ExpressionDataType.FLOAT,
    "float32": ExpressionDataType.FLOAT,
    "float64": ExpressionDataType.FLOAT,
    "f32": ExpressionDataType.FLOAT,
    "f64": ExpressionDataType.FLOAT,
    "double": ExpressionDataType.FLOAT,
    "bool": ExpressionDataType.BOOL,
    "boolean": ExpressionDataType.BOOL,
    "date": ExpressionDataType.DATE,
    "datetime": ExpressionDataType.DATE,
    "timestamp": ExpressionDataType.DATE,
    "null": ExpressionDataType.NULL,
}

_LIST_BY_SCALAR: dict[ExpressionDataType, ExpressionDataType] = {
    ExpressionDataType.STRING: ExpressionDataType.LIST_STRING,
    ExpressionDataType.INT: ExpressionDataType.LIST_INT,
    ExpressionDataType.FLOAT: ExpressionDataType.LIST_FLOAT,
    ExpressionDataType.BOOL: ExpressionDataType.LIST_BOOL,
    ExpressionDataType.DATE: ExpressionDataType.LIST_DATE,
}

_SCALAR_BY_LIST: dict[ExpressionDataType, ExpressionDataType] = {
    v: k for k, v in _LIST_BY_SCALAR.items()
}


def normalize_expression_type(value: str | ExpressionDataType) -> ExpressionDataType:
    if isinstance(value, ExpressionDataType):
        return value

    normalized = value.strip().lower().replace(" ", "")
    if not normalized:
        return ExpressionDataType.UNKNOWN

    if normalized in _TYPE_ALIASES:
        return _TYPE_ALIASES[normalized]

    if normalized.startswith("list(") and normalized.endswith(")"):
        inner = normalized[5:-1]
        return to_list_type(normalize_expression_type(inner))

    if normalized.startswith("list[") and normalized.endswith("]"):
        inner = normalized[5:-1]
        return to_list_type(normalize_expression_type(inner))

    if normalized.startswith("datetime["):
        return ExpressionDataType.DATE

    return ExpressionDataType.UNKNOWN


def to_list_type(value: ExpressionDataType) -> ExpressionDataType:
    return _LIST_BY_SCALAR.get(value, ExpressionDataType.UNKNOWN)


def list_item_type(value: ExpressionDataType) -> ExpressionDataType | None:
    return _SCALAR_BY_LIST.get(value)


def is_numeric(value: ExpressionDataType) -> bool:
    return value in {ExpressionDataType.INT, ExpressionDataType.FLOAT}


def is_scalar(value: ExpressionDataType) -> bool:
    return value in {
        ExpressionDataType.STRING,
        ExpressionDataType.INT,
        ExpressionDataType.FLOAT,
        ExpressionDataType.BOOL,
        ExpressionDataType.DATE,
        ExpressionDataType.NULL,
    }
