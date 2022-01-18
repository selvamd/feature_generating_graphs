package fgg.data;

//Note that any changes here need to be
//consistent with enum_types.py in the
//python system for grpc to work
public enum MsgType
{
	STATUS, LOGIN, GET_DATES,
	GET_NODES, GET_NODE_INFO,
	GET_EDGES, GET_EDGE_INFO,
	GET_ATTRS, GET_ATTR_INFO,
	GET_OBJ_KEYS, GET_OBJECT,
    GET_LINK_KEYS, GET_LEG_KEYS,
    TASK_REQUEST, ADD_ATTR,
	SET_OBJ_KEY, SET_LINK_KEY,
	NOTIFY_GIT_CHECKIN, NOTIFY_CBO_REFRESH,
    NOTIFY_FLUSH
}
