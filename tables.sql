drop table depara.codigo_pais;
create table depara.codigo_pais
(
    id             bigserial
    ,estado        text primary key
    ,codigo_pais   bigint
    ,created_at    timestamp default now()
);