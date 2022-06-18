create table evm_logs (
    id text not null primary key,
    -- {blockHeight-logIdx}
    address text not null,
    -- with index
    block_height numeric not null,
    -- with index
    topics0 text not null,
    -- with index
    topics1 text,
    topics2 text,
    topics3 text
);

create table evm_transactions (
    id text not null primary key,
    -- {blockHeight-txIdx}
    tx_hash text not null,
    -- with index
    "from" text not null,
    -- with index
    "to" text not null,
    -- with index
    func text,
    -- with index
    block_height numeric not null,
    -- with index
    success boolean not null
);