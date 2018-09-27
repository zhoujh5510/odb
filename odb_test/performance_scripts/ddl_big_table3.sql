create schema if not exists odb_performance_test;
drop table if exists odb_performance_test.big_table3;
create table if not exists odb_performance_test.big_table3 (id bigint not null, a int, b int, c varchar(20), d varchar(40), e varchar(60), f varchar(120), g varchar(260), h varchar(4000), primary key(id)) salt using 12 partitions on (id);
