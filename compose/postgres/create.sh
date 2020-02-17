#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS flows (
        id bigserial PRIMARY KEY,
        date_inserted timestamp default NULL,

        time_flow timestamp default NULL,
		flow_direction integer,
        type integer,
        sampling_rate integer,
        src_as bigint,
        dst_as bigint,
        src_ip inet,
        dst_ip inet,
        src_prj varchar(100),
        dst_prj varchar(100),
        bytes bigint,
        packets bigint,

        etype integer,
        proto integer,
        src_port integer,
        dst_port integer,
		sampler_address inet
    );
	create index ix_date_inserted on flows ( date_inserted);
EOSQL
