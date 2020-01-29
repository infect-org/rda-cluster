


create table rda_cluster_service."instance_shard" (
    id serial not null,
    "id_instance" int not null,
    "id_shard" int,
    created timestamp without time zone not null default now(),
    updated timestamp without time zone not null default now(),
    deleted timestamp without time zone,
    constraint "instance_shard_pk" 
        primary key (id),
    constraint "instance_shard_unique_pk"
        unique("id_instance", "id_shard"),
    constraint "instance_shard_fk_instance"
        foreign key ("id_instance")
        references "instance" (id)
        on update cascade
        on delete restrict,
    constraint "instance_shard_fk_shard"
        foreign key ("id_shard")
        references "shard" (id)
        on update cascade
        on delete restrict
);


alter table rda_cluster_service."instance" drop column "id_shard";
alter table rda_cluster_service."cluster" add column "modelPrefix" varchar(100);
