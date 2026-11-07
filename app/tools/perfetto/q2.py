import sys
from perfetto.trace_processor import TraceProcessor
tp = TraceProcessor(trace=sys.argv[1]); q=lambda s: tp.query(s).as_pandas_dataframe()
u="(select utid from thread where tid=3598)"
print(q(f"select state, sum(dur)/1e6 ms, count(*) n from thread_state where utid={u} group by state order by ms desc").to_string())
print(q(f"select blocked_function, sum(dur)/1e6 ms from thread_state where utid={u} and state in ('D','S') group by blocked_function order by ms desc limit 5").to_string())
# slices on render thread
print(q(f"""select s.name, count(*) n, sum(s.dur)/1e6 ms, max(s.dur)/1e6 max_ms from slice s join thread_track tt on s.track_id=tt.id
 where tt.utid={u} group by s.name order by ms desc limit 15""").to_string())
# per-second CPU of render thread (to see the pan window)
print(q(f"""select (s.ts-(select min(ts) from sched))/1000000000 as sec, sum(s.dur)/1e6 ms from sched s where s.utid={u} group by sec order by sec""").to_string())
# cpu7 freq
print(q("""select c.cpu, avg(c.value) avg_khz, max(c.value) max_khz from counter c join cpu_counter_track t on c.track_id=t.id where t.name='cpufreq' group by c.cpu""").to_string())
print(q("""select t.name, avg(c.value) avg, max(c.value) mx, count(*) n from counter c join counter_track t on c.track_id=t.id where t.name like '%gpu%' group by t.name""").to_string())
