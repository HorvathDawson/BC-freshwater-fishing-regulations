import sys
from perfetto.trace_processor import TraceProcessor
tp = TraceProcessor(trace=sys.argv[1]); q=lambda s: tp.query(s).as_pandas_dataframe()
print(q("""select t.cpu, avg(c.value)/1e3 avg_mhz, max(c.value)/1e3 max_mhz from counter c join cpu_counter_track t on c.track_id=t.id where t.name='cpufreq' group by t.cpu""").to_string())
print(q("""select t.name, avg(c.value) avg, max(c.value) mx from counter c join counter_track t on c.track_id=t.id where lower(t.name) like '%gpu%' group by t.name limit 8""").to_string())
