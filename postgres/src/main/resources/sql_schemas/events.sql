create table events
(
    eventStoreVersion bigserial not null,
    processid         uuid not null,
    aggregateid       uuid not null,
    aggregatename     text not null,
    sentdate          text not null,
    aggregateVersion  int not null,
    payload           text not null
);

alter table events add primary key (eventStoreVersion);
create unique index on events USING btree (aggregateid, aggregateVersion);
create index on events (aggregatename, aggregateid);