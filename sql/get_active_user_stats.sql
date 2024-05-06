with dummy(temporaer) as (
select TIMESTAMP('$time_from$') from SYSIBM.SYSDUMMY1
union all
select temporaer + 1 MINUTES from dummy where temporaer < TIMESTAMP('$time_end$')
),
mintues_counts as (select temporaer,count (distinct viewer_person_id) as counts from dummy left join pcm.page_view_info on substr(temporaer,1,16)=substr(CREATE_TS,1,16)
group by temporaer order by temporaer ASC),

aggregate_total as (select row_number() over (order by temporaer) as ID, TEMPORAER, sum(counts) over (order by temporaer rows between current row and 4 following) as total_counts from mintues_counts)

select * from aggregate_total where mod(ID,5)=1;