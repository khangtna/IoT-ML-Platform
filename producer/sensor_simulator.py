import argparse,csv,json,os,time
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer
NULL={"","NA","NaN","nan",None}
def nf(v): return None if v in NULL else float(v)
def event(r):
 ts=datetime(int(r["year"]),int(r["month"]),int(r["day"]),int(r["hour"])).isoformat()+"+08:00"; s=r["station"]
 return {"event_id":f"{s}-{ts}","station_id":s,"timestamp":ts,"pm25":nf(r.get("PM2.5")),"pm10":nf(r.get("PM10")),"so2":nf(r.get("SO2")),"no2":nf(r.get("NO2")),"co":nf(r.get("CO")),"o3":nf(r.get("O3")),"temperature":nf(r.get("TEMP")),"pressure":nf(r.get("PRES")),"dew_point":nf(r.get("DEWP")),"rainfall":nf(r.get("RAIN")),"wind_direction":None if r.get("wd") in NULL else r.get("wd"),"wind_speed":nf(r.get("WSPM"))}
def rows(d):
 fs=sorted(Path(d).glob("*.csv"));
 if not fs: raise SystemExit(f"No CSV files under {d}")
 for f in fs:
  with f.open(encoding="utf-8-sig",newline="") as h: yield from csv.DictReader(h)
def main():
 p=argparse.ArgumentParser(); p.add_argument("--data-dir",required=True); p.add_argument("--events-per-second",type=int,default=int(os.getenv("EVENTS_PER_SECOND","500"))); a=p.parse_args()
 pr=KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092"),key_serializer=lambda v:v.encode(),value_serializer=lambda v:json.dumps(v).encode(),acks="all",linger_ms=10,batch_size=32768)
 n=0; t=time.perf_counter(); rate=max(a.events_per_second,1)
 for r in rows(a.data_dir):
  e=event(r); pr.send(os.getenv("KAFKA_TOPIC","sensor-events"),key=e["station_id"],value=e); n+=1
  wait=n/rate-(time.perf_counter()-t)
  if wait>0: time.sleep(wait)
  if n%rate==0: print(f"sent={n} avg_rate={n/(time.perf_counter()-t):.2f} events/s")
 pr.flush(); pr.close()
if __name__=="__main__": main()
