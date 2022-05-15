create table events
(
    id           text    not null        primary key, -- {blockHeight-eventIdx}
    module       text    not null,  -- with index
    event        text    not null,  -- with index
    block_height numeric not null  -- with index
);
create table extrinsics
(
    id           text    not null        primary key, -- {blockHeight-extrinsicIdx}
    tx_hash      text    not null,  -- with index
    module       text    not null,  -- with index
    call         text    not null,  -- with index
    block_height numeric not null,  -- with index
    success      boolean not null,
    is_signed    boolean not null
);
create table spec_versions
(
    id           text    not null        primary key, -- {specVersion}
    block_height numeric not null
);
create table evm_logs
(
    id           text    not null        primary key, -- {blockHeight-logIdx}
    address      text    not null,  -- with index
    block_height numeric not null,  -- with index
    topics0      text    not null,  -- with index
    topics1      text,
    topics2      text,
    topics3      text
);
create table evm_transactions
(
    id           text    not null        primary key, -- {blockHeight-txIdx}
    tx_hash      text    not null,  -- with index
    "from"       text    not null,  -- with index
    "to"         text    not null,  -- with index
    func         text,  -- with index
    block_height numeric not null,  -- with index
    success      boolean not null
);