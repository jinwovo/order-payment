create table product (
    id          bigserial primary key,
    sku         varchar(64)  not null unique,
    name        varchar(255) not null,
    price_cents bigint       not null check (price_cents >= 0),
    stock       integer      not null check (stock >= 0)
);

create table orders (
    id             uuid primary key,
    status         varchar(32) not null,
    total_cents    bigint      not null,
    failure_reason varchar(255),
    created_at     timestamptz not null,
    updated_at     timestamptz not null
);

create table order_item (
    id               bigserial primary key,
    order_id         uuid    not null references orders (id),
    product_id       bigint  not null references product (id),
    sku              varchar(64) not null,
    quantity         integer not null check (quantity > 0),
    unit_price_cents bigint  not null
);
create index idx_order_item_order on order_item (order_id);

create table payment (
    id           uuid primary key,
    order_id     uuid        not null references orders (id),
    status       varchar(32) not null,
    amount_cents bigint      not null,
    psp_ref      varchar(64),
    created_at   timestamptz not null
);
create index idx_payment_order on payment (order_id);

-- Maps an Idempotency-Key to the order it produced, so a retried request returns the same order.
create table idempotency_key (
    id         varchar(128) primary key,
    order_id   uuid        not null,
    created_at timestamptz not null
);

-- Transactional outbox: events are written in the same DB tx as the state change, then relayed.
create table outbox_event (
    id             bigserial primary key,
    aggregate_type varchar(64) not null,
    aggregate_id   varchar(64) not null,
    type           varchar(64) not null,
    payload        text        not null,
    created_at     timestamptz not null,
    published_at   timestamptz
);
create index idx_outbox_unpublished on outbox_event (id) where published_at is null;

-- Seed catalog.
insert into product (sku, name, price_cents, stock) values
    ('SKU-LAPTOP', 'Laptop',        150000, 10),
    ('SKU-MOUSE',  'Mouse',           2500, 100),
    ('SKU-DESK',   'Standing Desk',  45000, 5);
