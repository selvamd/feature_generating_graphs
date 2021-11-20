create table if not exists cbo_dict (
    recordid int AUTO_INCREMENT,
    lookup_name varchar(100),
    lookup_value varchar(100),
    PRIMARY KEY (recordid),
    CONSTRAINT UC_data_dict UNIQUE (lookup_name,lookup_value)
);

create table if not exists dbo_node (
    recordid int,
    from_dt int,
    to_dt int,
    node_key int,
    node_name varchar(100),
    parent_node_key int,
    root_node_key int,
    leg_node_key int,
    PRIMARY KEY (node_key),
    CONSTRAINT UC_node UNIQUE (node_name)
);

create table if not exists dbo_edge (
    recordid int,
    from_dt int,
    to_dt int,
    edge_key int,
    edge_name varchar(100),
    default_edge int,
    node_size int,
    node_key0 int,
    node_key1 int,
    node_key2 int,
    node_key3 int,
    node_key4 int,
    node_limits0 int,
    node_limits1 int,
    node_limits2 int,
    node_limits3 int,
    node_limits4 int,
    PRIMARY KEY (edge_key),
    CONSTRAINT UC_edge UNIQUE (edge_name)
);

create table if not exists dbo_node_tree (
    recordid int,
    from_dt int,
    to_dt int,
    parent_node_key int,
    child_node_key  int,
    PRIMARY KEY (parent_node_key,child_node_key)
);

create table if not exists dbo_object (
    recordid int,
    from_dt int,
    to_dt int,
    object_key int,
    node_key int,
    root_node_key   int,
    alt_key_number  int,
    alt_key_value   varchar(255),
    PRIMARY KEY (node_key,alt_key_number,object_key)
);

create table if not exists dbo_object_leg (
    recordid  int ,
    from_dt  int,
    to_dt  int,
    leg_key int,
    group_node_key   int,
    group_object_key int,
    leg_object_key int,
    PRIMARY KEY (leg_key,from_dt)
);

create table if not exists dbo_links (
    recordid int ,
    from_dt int,
    to_dt int,
    link_key int,
    edge_key int,
    object_key0 int,
    object_key1 int,
    object_key2 int,
    object_key3 int,
    object_key4 int,
    PRIMARY KEY (link_key,from_dt)
);

create table if not exists dbo_attr (
    recordid int,
    from_dt int,
    to_dt int,
    attr_key int,
    attr_name varchar(100),
    dtype varchar(10),
    refresh_type    varchar(10),
    maxsize      int,
    node_key int,
    edge_key int,
    inputs   varchar(255),
    src_file   varchar(255),
    fill_all_key     int,
    fill_all_time    int,
    is_deploy int,
    is_curational int,
    is_pii int,
    is_enum          int,
    split_by_leg int,
    alt_key_number   int,
    link_path    varchar(255),
    filter_expr   varchar(1024),
    reducer varchar(100),
    PRIMARY KEY (attr_key)
);

create table if not exists dbo_git_history (
    recordid int,
    from_dt int,
    to_dt int,
    attr_key int,
    src_version int,
    src_time datetime,
    PRIMARY KEY (recordid)
);

create table if not exists dbo_attr_fill_state (
    recordid int,
    from_dt int,
    to_dt int,
    attr_key    int,
    object_key  int,
    PRIMARY KEY (recordid)
);
