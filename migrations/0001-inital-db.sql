drop schema if exists rda_cluster_service cascade;
create schema  if not exists rda_cluster_service;

set search_path to rda_cluster_service;





create table rda_cluster_service."clusterStatus" (
    id serial not null,
    identifier varchar(100) not null,
    constraint "clusterStatus_pk" 
        primary key (id),
    constraint "clusterStatus_unique_identifier"
        unique ("identifier")
);


create table rda_cluster_service."cluster" (
    id serial not null,
    "id_clusterStatus" int not null,
    identifier varchar(100) not null,
    "dataSetIdentifier" varchar(100) not null,
    "dataSource" varchar(100) not null,
    constraint "cluster_pk" 
        primary key (id),
    constraint "cluster_unique_identifier"
        unique ("identifier"),
    constraint "cluster_fk_clusterStatus"
        foreign key ("id_clusterStatus")
        references "clusterStatus" (id)
        on update cascade
        on delete restrict
);

create table rda_cluster_service."shard" (
    id serial not null,
    id_cluster int not null,
    identifier varchar(100) not null,
    created timestamp without time zone not null default now(),
    updated timestamp without time zone not null default now(),
    deleted timestamp without time zone,
    constraint "shard_pk" 
        primary key (id),
    constraint "shard_unique_identifier"
        unique ("identifier"),
    constraint "shard_fk_cluster"
        foreign key ("id_cluster")
        references "cluster" (id)
        on update cascade
        on delete restrict
);


create table rda_cluster_service."node" (
    id serial not null,
    identifier varchar(100) not null,
    created timestamp without time zone not null default now(),
    updated timestamp without time zone not null default now(),
    deleted timestamp without time zone,
    constraint "node_pk" 
        primary key (id),
    constraint "node_unique_identifier"
        unique ("identifier")
);


create table rda_cluster_service."instance" (
    id serial not null,
    "id_node" int not null,
    "id_shard" int,
    identifier varchar(100) not null,
    memory bigint not null,
    "loadedRecordCount" int not null default 0,
    url varchar(300) not null,
    created timestamp without time zone not null default now(),
    updated timestamp without time zone not null default now(),
    deleted timestamp without time zone,
    constraint "instance_pk" 
        primary key (id),
    constraint "instance_unique_identifier"
        unique ("identifier"),
    constraint "instance_fk_node"
        foreign key ("id_node")
        references "node" (id)
        on update cascade
        on delete restrict,
    constraint "instance_fk_shard"
        foreign key ("id_shard")
        references "shard" (id)
        on update cascade
        on delete restrict
);


insert into rda_cluster_service."clusterStatus" ("identifier") values ('created');
insert into rda_cluster_service."clusterStatus" ("identifier") values ('initialized');
insert into rda_cluster_service."clusterStatus" ("identifier") values ('active');
insert into rda_cluster_service."clusterStatus" ("identifier") values ('ended');
insert into rda_cluster_service."clusterStatus" ("identifier") values ('failed');