create table events (
    id text not null primary key,
    -- {blockHeight-eventIdx}
    module text not null,
    -- with index
    event text not null,
    -- with index
    block_height numeric not null -- with index
);

create table extrinsics (
    id text not null primary key,
    -- {blockHeight-extrinsicIdx}
    tx_hash text not null,
    -- with index
    module text not null,
    -- with index
    call text not null,
    -- with index
    block_height numeric not null,
    -- with index
    success boolean not null,
    is_signed boolean not null
);

create table spec_versions (
    id text not null primary key,
    -- {specVersion}
    block_height numeric not null
);

create table _metadata (
    key varchar(255) not null primary key,
    value jsonb,
    "createdAt" timestamp with time zone not null,
    "updatedAt" timestamp with time zone not null
);