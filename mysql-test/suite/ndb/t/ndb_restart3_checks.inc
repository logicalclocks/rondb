--echo Check row count
select count(1) from test.t1 into @row_count;
select @row_count;

--echo Check Physical row count against expected
select sum(fixed_elem_count) from ndbinfo.memory_per_fragment where fq_name='test/def/t1' into @sum;
set @diff = (@row_count * @prpr) - @sum;
select floor(@row_count * @prpr) as expected_phys_rows,
       @sum as actual_phys_rows,
       @diff as diff;

--let $d=query_get_value(select @diff as d, d, 1)

if ($d)
{
  --echo Show actual fragments
  select * from ndbinfo.memory_per_fragment
   where fq_name='test/def/t1'
   order by
         node_id, fragment_num;
  --die Physical vs Logical row count error
}

--echo Check UI physical row count against expected
select sum(fixed_elem_count) from ndbinfo.memory_per_fragment where parent_fq_name='test/def/t1' and fq_name like '%/c$unique' into @sum;
set @diff = (@row_count * @prpr) - @sum;
select floor(@row_count * @prpr) as expected_phys_rows,
       @sum as actual_phys_rows,
       @diff as diff;

delete from test.t1 limit 512;

--disable_query_log
--disable_result_log
select @m:=max(a) from test.t1;
insert into test.t1 select a+@m, b+@m, c+@m, d+@m  from test.t1 limit 800;
--enable_query_log
--enable_result_log

create table test.t2(a int primary key) engine=ndb;
insert into test.t2 values (1);
drop table test.t2;
