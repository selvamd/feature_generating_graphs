"""Container for all enum datatypes used for streaming"""
from enum import IntEnum
from enum import Enum

class XMIT(IntEnum):
    """Represents the xmit types as int enum."""
    KEYSEQ = 0
    VALUEDT = 1
    VALUE = 2
    ATTRKEY = 3

class FieldType(IntEnum):
    """Represents the fieldtype as int enum."""
    CORE    = 0
    STATIC  = 1
    DYNAMIC = 2
    VIRTUAL = 3

class MsgType(IntEnum):
    """Represents the msgtype as int enum."""
    STATUS = 0
    LOGIN = 1
    GET_DATES = 2
    GET_NODES = 3
    GET_NODE_INFO = 4
    GET_EDGES = 5
    GET_EDGE_INFO = 6
    GET_ATTRS = 7
    GET_ATTR_INFO = 8
    GET_OBJ_KEYS = 9
    GET_OBJECT = 10
    GET_LINK_KEYS = 11
    GET_LEG_KEYS = 12
    TASK_REQUEST = 13
    ADD_ATTR = 14
    SET_OBJ_KEY = 15
    SET_LINK_KEY = 16
    NOTIFY_GIT_CHECKIN = 17
    NOTIFY_CBO_REFRESH = 18

class DataType(Enum):
    """Represents the datatype as int enum."""
    BYTE = 0
    CHAR = 1
    SHORT = 2
    INT = 3
    LONG = 4
    FLOAT = 5
    DOUBLE = 6
    STRING = 7
    DATE = 8
    KEY = 9
    ENUM = 10
