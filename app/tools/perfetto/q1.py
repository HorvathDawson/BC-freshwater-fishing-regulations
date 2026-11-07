import sys
from perfetto.trace_processor import TraceProcessor
tp = TraceProcessor(trace=sys.argv[1])
q=lambda s: tp.query(s).as_pandas_dataframe()
print(q("select (max(ts)-min(ts))/1e9 as secs from sched"))
pr=q("select upid,pid,name from process where name like '%canifish%'"); print(pr)
upid=int(pr.upid.iloc[0])
# CPU time per thread of the app
print(q(f"""select t.name, t.tid, sum(s.dur)/1e6 as cpu_ms, count(*) n from sched s join thread t using(utid)
 where t.upid={upid} group by t.utid order by cpu_ms desc limit 15""").to_string())
# which cpus the top thread ran on
print(q(f"""select s.cpu, sum(s.dur)/1e6 ms from sched s join thread t using(utid) where t.upid={upid}
 group by s.cpu order by s.cpu""").to_string())
# frame timeline
try:
  print(q(f"""select count(*) frames, sum(case when jank_type!='None' then 1 else 0 end) janky,
   avg(dur)/1e6 avg_ms from actual_frame_timeline_slice where upid={upid}""").to_string())
  print(q(f"""select jank_type, count(*) n from actual_frame_timeline_slice where upid={upid} group by jank_type order by n desc""").to_string())
except Exception as e: print('no frame timeline', e)
