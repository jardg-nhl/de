#!/usr/bin/env python3
"""CineInsight local medallion pipeline; standard-library only."""
from __future__ import annotations
import argparse, csv, hashlib, json, math, re, shutil, sqlite3, sys, uuid, unicodedata, string
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = json.loads((ROOT / "config" / "pipeline.json").read_text(encoding="utf-8"))
SOURCE = ROOT / CONFIG["paths"]["source"]
LAKE = ROOT / CONFIG["paths"]["lakehouse"]
DB = ROOT / CONFIG["paths"]["control_database"]
LEGACY_DB = LAKE / "cineinsight.db"
TABLES = ["movie", "link", "genome_tags", "genome_scores", "rating", "tag"]
EXPECTED_ROWS = CONFIG["expected_rows"]
DDL = """
CREATE TABLE IF NOT EXISTS control(batch_id TEXT PRIMARY KEY, source_file TEXT, source_system TEXT, event_from TEXT, event_to TEXT, status TEXT, rows_read INTEGER DEFAULT 0, rows_written INTEGER DEFAULT 0, rows_quarantined INTEGER DEFAULT 0, checksum TEXT, watermark TEXT, started_at TEXT, finished_at TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS watermarks(source_file TEXT PRIMARY KEY, event_time TEXT, last_batch TEXT);
CREATE TABLE IF NOT EXISTS bronze(source_file TEXT, source_system TEXT, batch_id TEXT, ingested_at TEXT, row_number INTEGER, record_hash TEXT, payload TEXT, PRIMARY KEY(source_file,batch_id,row_number));
CREATE TABLE IF NOT EXISTS silver_processed(source_file TEXT,batch_id TEXT,row_number INTEGER,processed_at TEXT,PRIMARY KEY(source_file,batch_id,row_number));
CREATE INDEX IF NOT EXISTS ix_bronze_hash ON bronze(source_file,record_hash);
CREATE TABLE IF NOT EXISTS silver_events(source_system TEXT, event_id TEXT PRIMARY KEY, party_id INTEGER, content_id INTEGER, event_type TEXT, event_value REAL, event_time_utc TEXT, source_file TEXT, batch_id TEXT, record_hash TEXT, event_text TEXT);
CREATE INDEX IF NOT EXISTS ix_event_time ON silver_events(event_time_utc);
CREATE TABLE IF NOT EXISTS silver_movie(movie_id INTEGER PRIMARY KEY, title TEXT, release_year INTEGER, genres_json TEXT, is_deleted INTEGER NOT NULL DEFAULT 0, source_file TEXT, batch_id TEXT, record_hash TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS dim_movie(movie_sk INTEGER PRIMARY KEY AUTOINCREMENT, movie_id INTEGER NOT NULL, title TEXT, release_year INTEGER, genres_json TEXT, is_deleted INTEGER NOT NULL, effective_from TEXT NOT NULL, effective_to TEXT, is_current INTEGER NOT NULL, version INTEGER NOT NULL, previous_title TEXT, changed_date TEXT, source_file TEXT, batch_id TEXT, record_hash TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_movie_current ON dim_movie(movie_id) WHERE is_current=1;
CREATE TABLE IF NOT EXISTS dim_user(user_sk INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER UNIQUE NOT NULL, first_seen TEXT, source_file TEXT, batch_id TEXT);
CREATE TABLE IF NOT EXISTS fact_interaction(event_id TEXT PRIMARY KEY, user_sk INTEGER, movie_sk INTEGER, party_id INTEGER, content_id INTEGER, event_type TEXT, event_value REAL, event_time_utc TEXT, source_file TEXT, batch_id TEXT, record_hash TEXT, event_text TEXT);
CREATE INDEX IF NOT EXISTS ix_fact_time ON fact_interaction(event_time_utc);
CREATE TABLE IF NOT EXISTS fact_genome(movie_id INTEGER, tag_id INTEGER, relevance REAL, source_file TEXT, batch_id TEXT, record_hash TEXT, PRIMARY KEY(movie_id,tag_id));
CREATE TABLE IF NOT EXISTS dim_genome_tag(tag_id INTEGER PRIMARY KEY, tag TEXT, source_file TEXT, batch_id TEXT, record_hash TEXT);
CREATE TABLE IF NOT EXISTS dim_movie_link(movie_id INTEGER PRIMARY KEY, imdb_id INTEGER, tmdb_id INTEGER, imdb_url TEXT, tmdb_url TEXT, source_file TEXT, batch_id TEXT, record_hash TEXT);
CREATE TABLE IF NOT EXISTS dq_results(batch_id TEXT, rule_id TEXT, severity TEXT, passed INTEGER, observed REAL, threshold REAL, detail TEXT);
CREATE TABLE IF NOT EXISTS quarantine(batch_id TEXT, source_file TEXT, row_number INTEGER, error_code TEXT, reason TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS lineage(gold_table TEXT, gold_key TEXT, source_file TEXT, batch_id TEXT, record_hash TEXT, PRIMARY KEY(gold_table,gold_key,source_file,batch_id));
"""
NOW = lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")

def connect():
    LAKE.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB, timeout=120)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.executescript(DDL)
    return c

def digest(v):
    return hashlib.sha256(json.dumps(v, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()

def init():
    (LAKE / "landing").mkdir(parents=True, exist_ok=True)
    (LAKE / "reports").mkdir(parents=True, exist_ok=True)
    DB.parent.mkdir(parents=True, exist_ok=True)
    if not DB.exists() and LEGACY_DB.exists():
        shutil.copy2(LEGACY_DB, DB)
    with connect() as c:
        for name in TABLES:
            src = SOURCE / f"{name}.csv"
            if not src.is_file(): raise FileNotFoundError(src)
            target = LAKE / "landing" / f"{name}.csv"
            if not target.exists(): shutil.copyfile(src, target)
            elif sha_file(src) != sha_file(target): raise RuntimeError(f"Immutable landing conflict: {target}")
    print(f"Landing verified under {LAKE / 'landing'}")

def sha_file(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def parse_year(title):
    m=re.search(r"\((\d{4})\)\s*$", title or "")
    return int(m.group(1)) if m else None

def normalize_genres(s):
    if s == "(no genres listed)": return []
    return [x.strip() for x in (s or "").split("|") if x.strip()]

def parse_ts(raw):
    try:
        if raw is None: return None
        try: return datetime.fromtimestamp(int(raw),timezone.utc).isoformat(timespec="seconds")
        except ValueError:
            value=datetime.strptime(raw.strip(),"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return value.isoformat(timespec="seconds")
    except (ValueError,TypeError,OverflowError): return None

def batch_id(name, lo, hi): return hashlib.sha1(f"{name}|{lo}|{hi}".encode()).hexdigest()[:20]

def record_batch(c, name, lo, hi, limit=None):
    path=LAKE/"landing"/f"{name}.csv"; bid=batch_id(name,lo,hi); started=NOW()
    already=c.execute("SELECT status FROM control WHERE batch_id=?",(bid,)).fetchone()
    if already and already[0]=="SUCCESS": return 0,0,bid
    c.execute("INSERT OR REPLACE INTO control(batch_id,source_file,source_system,event_from,event_to,status,started_at) VALUES(?,?,?,?,?,'RUNNING',?)",(bid,name,"movielens",lo,hi,started)); c.commit()
    h=hashlib.sha256(); read=written=bad=0; chunk=[]; rownum=0
    with open(path,encoding="utf-8-sig",newline="",errors="replace") as f:
        for row in csv.DictReader(f):
            rownum+=1
            if name in ("rating","tag"):
                ts=parse_ts(row.get("timestamp"));
                if ts is None: continue
                if lo and ts < lo: continue
                if hi and ts >= hi: continue
            else:
                if c.execute("SELECT 1 FROM watermarks WHERE source_file=?",(name,)).fetchone(): break
            read+=1; rh=digest(row); h.update(rh.encode())
            chunk.append((name,"movielens",bid,NOW(),rownum,rh,json.dumps(row,ensure_ascii=False,separators=(",",":"))))
            if len(chunk)>=limit:
                c.executemany("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",chunk); written+=len(chunk); chunk=[]
    if chunk: c.executemany("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",chunk); written+=len(chunk)
    checksum=sha_file(path)
    c.execute("INSERT OR REPLACE INTO control(batch_id,source_file,source_system,event_from,event_to,status,rows_read,rows_written,rows_quarantined,checksum,watermark,started_at,finished_at) VALUES(?,?,?, ?,?,'SUCCESS',?,?,?,?,?,?,?)",(bid,name,"movielens",lo,hi,read,read, bad,checksum,hi,started,NOW()))
    c.execute("INSERT OR REPLACE INTO watermarks VALUES(?,?,?)",(name,hi,bid)); c.commit()
    return read,bad,bid

def events_to_silver(c,bid,name):
    cur=c.execute("SELECT b.row_number,b.payload,b.record_hash FROM bronze b LEFT JOIN silver_processed p ON p.source_file=b.source_file AND p.batch_id=b.batch_id AND p.row_number=b.row_number WHERE b.batch_id=? AND p.row_number IS NULL ORDER BY b.row_number",(bid,))
    total=bad=hard_bad=0; src=f"{name}.csv"
    while True:
        rows=cur.fetchmany(50000)
        if not rows: break
        out=[]; quarant=[]
        for rn,payload,rh in rows:
            d=json.loads(payload)
            try:
                if name=="rating":
                    user=int(d["userId"]); movie=int(d["movieId"]); val=float(d["rating"]); ts=parse_ts(d["timestamp"])
                    if val<.5 or val>5 or round(val*2)!=val*2: raise ValueError("RATING_RANGE_OR_STEP")
                    typ="RATING"; value=val*20; event_text=None
                else:
                    user=int(d["userId"]); movie=int(d["movieId"]); val=(d.get("tag") or "").strip(); ts=parse_ts(d["timestamp"])
                    if not val: raise ValueError("EMPTY_TAG")
                    typ="TAG"; value=None; event_text=val
                if not ts: raise ValueError("INVALID_TIMESTAMP")
                eid=digest(["movielens",name,user,movie,d.get("timestamp"),d.get("rating"),d.get("tag")])
                out.append(("movielens",eid,user,movie,typ,value,ts,src,bid,rh,event_text))
            except Exception as e:
                quarant.append((bid,src,rn,str(e),str(e),payload))
                if str(e)!="EMPTY_TAG": hard_bad+=1
        c.executemany("INSERT INTO silver_events VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET event_value=excluded.event_value,event_text=excluded.event_text,event_time_utc=excluded.event_time_utc,record_hash=excluded.record_hash WHERE excluded.event_time_utc>=silver_events.event_time_utc",out)
        c.executemany("INSERT INTO quarantine VALUES(?,?,?,?,?,?)",quarant)
        c.executemany("INSERT OR IGNORE INTO dq_results VALUES(?,?,?,?,?,?,?)",[(bid,"event_valid", "WARNING" if e[3]=="EMPTY_TAG" else "BLOCKING",0,None,None,e[4]) for e in quarant])
        c.executemany("INSERT OR IGNORE INTO silver_processed VALUES(?,?,?,?)",[(name,bid,r[0],NOW()) for r in rows])
        c.commit(); total+=len(out); bad+=len(quarant)
    c.execute("INSERT OR REPLACE INTO dq_results VALUES(?,?,?,?,?,?,?)",(bid,"event_payload_valid","BLOCKING",int(hard_bad==0),hard_bad,0,"invalid timestamp, identifier, or rating domain/step; blank free-text tags are warnings")); c.commit()
    return total,bad

def assert_no_blocking_failures(c):
    failures=c.execute("SELECT DISTINCT rule_id,observed,detail FROM dq_results WHERE severity='BLOCKING' AND passed=0").fetchall()
    if failures: raise RuntimeError("Blocking data quality failure: "+"; ".join(f"{r[0]} ({r[1]}): {r[2]}" for r in failures[:20]))

def ingest_event_years(c,name,cutoff,chunk_size):
    """Read each large event CSV once, route rows to deterministic event-year batches."""
    path=LAKE/"landing"/f"{name}.csv"; started=NOW(); checksum=sha_file(path); counts=Counter(); bounds={}; chunk=[]
    with open(path,encoding="utf-8-sig",newline="",errors="replace") as f:
        for rn,row in enumerate(csv.DictReader(f),1):
            ts=parse_ts(row.get("timestamp"))
            if ts is None: continue
            year=int(ts[:4]); lo=f"{year:04d}-01-01T00:00:00+00:00"; hi=f"{year+1:04d}-01-01T00:00:00+00:00"
            if cutoff and ts[:10]>=cutoff: continue
            bid=batch_id(name,lo,hi); counts[bid]+=1; bounds[bid]=(lo,hi)
            chunk.append((name,"movielens",bid,started,rn,digest(row),json.dumps(row,ensure_ascii=False,separators=(",",":"))))
            if len(chunk)>=chunk_size:
                c.executemany("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",chunk); c.commit(); chunk=[]
        if chunk: c.executemany("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",chunk); c.commit()
    observed=sum(counts.values())
    if cutoff is None and observed!=EXPECTED_ROWS[name]: raise RuntimeError(f"Landing reconciliation failed for {name}: expected {EXPECTED_ROWS[name]}, read {observed}")
    if cutoff and cutoff>="2016-01-01" and observed!=EXPECTED_ROWS[name]: raise RuntimeError(f"Landing reconciliation failed for {name}: expected {EXPECTED_ROWS[name]}, read {observed}")
    for bid,nrows in counts.items():
        lo_s,hi_s=bounds[bid]
        c.execute("INSERT OR REPLACE INTO control(batch_id,source_file,source_system,event_from,event_to,status,rows_read,rows_written,checksum,watermark,started_at,finished_at) VALUES(?,?,?,?,?,'SUCCESS',?,?,?,?,?,?)",(bid,name,"movielens",lo_s,hi_s,nrows,nrows,checksum,hi_s,started,NOW()))
        old=c.execute("SELECT event_time FROM watermarks WHERE source_file=?",(name,)).fetchone()
        if not old or hi_s>old[0]: c.execute("INSERT OR REPLACE INTO watermarks VALUES(?,?,?)",(name,hi_s,bid))
        c.commit(); events_to_silver(c,bid,name)

def synthesize_movie_changes(c):
    path=LAKE/"landing"/"movie.csv"; bid=batch_id("movie_changes","snapshot","v1")
    if c.execute("SELECT 1 FROM control WHERE batch_id=? AND status='SUCCESS'",(bid,)).fetchone(): return
    allrows=[]
    with open(path,encoding="utf-8-sig",newline="",errors="replace") as f:
        allrows=list(csv.DictReader(f))
    if len(allrows)!=EXPECTED_ROWS["movie"]: raise RuntimeError(f"Landing reconciliation failed for movie: expected {EXPECTED_ROWS['movie']}, read {len(allrows)}")
    # Catalog snapshot has no valid-from timestamp; use an explicit baseline convention before all observed events.
    stamp="1900-01-01T00:00:00+00:00"
    for i,r in enumerate(allrows,1):
        rh=digest(r); c.execute("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",("movie","movielens",bid,NOW(),i,rh,json.dumps(r,ensure_ascii=False)))
    c.commit(); assert_no_blocking_failures(c)
    changes=[]; n=len(allrows)
    for j,r in enumerate(allrows):
        mid=int(r["movieId"])
        if j%5000==0:
            d=dict(r); d["movieId"]=str(1000000+mid); d["title"]=r["title"]+" (CineInsight Added)"; changes.append(("INSERT",d))
        if j%5000==1:
            d=dict(r); title=d.get("title") or ""; d["title"]=re.sub(r"\s+(?=\(\d{4}\)\s*$)"," Director's Cut ",title) if re.search(r"\(\d{4}\)\s*$",title) else title+" Director's Cut"; d["genres"]=(r["genres"]+"|Drama") if r["genres"] and r["genres"]!="(no genres listed)" and "Drama" not in r["genres"] else r["genres"]; changes.append(("UPDATE",d))
        if j%5000==2: changes.append(("DELETE",dict(r)))
    chbid=batch_id("movie_changes","synthetic","v1")
    for i,(op,r) in enumerate(changes,1):
        r["_operation"]=op; r["_change_time"]=f"2026-01-{1+(i%28):02d}T00:00:00+00:00"; rh=digest(r)
        c.execute("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",("movie", "synthetic-cdc",chbid,NOW(),i,rh,json.dumps(r,ensure_ascii=False)))
    c.execute("INSERT OR REPLACE INTO control(batch_id,source_file,source_system,event_from,event_to,status,rows_read,rows_written,checksum,watermark,started_at,finished_at) VALUES(?,?,?,?,?,'SUCCESS',?,?,?,?,?,?)",(bid,"movie.csv","movielens",None,None,len(allrows),len(allrows),sha_file(path),stamp,NOW(),NOW()))
    c.execute("INSERT OR REPLACE INTO control(batch_id,source_file,source_system,event_from,event_to,status,rows_read,rows_written,checksum,watermark,started_at,finished_at) VALUES(?,?,?,?,?,'SUCCESS',?,?,?,?,?,?)",(chbid,"movie.csv","synthetic-cdc",None,None,len(changes),len(changes),None,"2026-01-28",NOW(),NOW()))
    c.commit()

def movies_to_gold(c):
    rows=c.execute("SELECT batch_id,row_number,payload,record_hash,source_system FROM bronze WHERE source_file='movie' ORDER BY ingested_at,row_number").fetchall()
    for bid,rn,payload,rh,system in rows:
        d=json.loads(payload); op=d.pop("_operation","BASELINE"); effective=d.pop("_change_time",None) or "1900-01-01T00:00:00+00:00"
        mid=int(d["movieId"]); title=(d.get("title") or "").strip(); year=parse_year(title); genres=normalize_genres(d.get("genres")); deleted=int(op=="DELETE")
        old=c.execute("SELECT movie_id,title,release_year,genres_json,is_deleted FROM silver_movie WHERE movie_id=?",(mid,)).fetchone()
        c.execute("INSERT INTO silver_movie VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(movie_id) DO UPDATE SET title=excluded.title,release_year=excluded.release_year,genres_json=excluded.genres_json,is_deleted=excluded.is_deleted,batch_id=excluded.batch_id,record_hash=excluded.record_hash,updated_at=excluded.updated_at WHERE excluded.record_hash<>silver_movie.record_hash",(mid,title,year,json.dumps(genres),deleted,"movie.csv",bid,rh,effective))
        cur=c.execute("SELECT movie_sk,title,release_year,genres_json,is_deleted,version FROM dim_movie WHERE movie_id=? AND is_current=1",(mid,)).fetchone()
        if not cur:
            c.execute("INSERT INTO dim_movie(movie_id,title,release_year,genres_json,is_deleted,effective_from,is_current,version,source_file,batch_id,record_hash) VALUES(?,?,?,?,?,?,1,1,'movie.csv',?,?)",(mid,title,year,json.dumps(genres),deleted,effective,bid,rh))
        else:
            title_changed=(cur[1],cur[2])!=(title,year)
            type2_changed=(cur[3],cur[4])!=(json.dumps(genres),deleted)
            # Apply Type 1 corrections even if the same CDC row also changes a Type 2 attribute.
            if title_changed:
                c.execute("UPDATE dim_movie SET title=?,release_year=? WHERE movie_id=?",(title,year,mid))
            if type2_changed:
                c.execute("UPDATE dim_movie SET effective_to=?,is_current=0 WHERE movie_sk=?",(effective,cur[0]))
                c.execute("INSERT INTO dim_movie(movie_id,title,release_year,genres_json,is_deleted,effective_from,is_current,version,previous_title,changed_date,source_file,batch_id,record_hash) VALUES(?,?,?,?,?,?,1,?,?,?,'movie.csv',?,?)",(mid,title,year,json.dumps(genres),deleted,effective,cur[5]+1,cur[1],effective if title_changed else None,bid,rh))
            elif title_changed:
                c.execute("UPDATE dim_movie SET previous_title=?,changed_date=? WHERE movie_sk=?",(cur[1],effective,cur[0]))
        c.execute("INSERT OR IGNORE INTO lineage VALUES('dim_movie',?,?,?,?)",(str(mid),"movie.csv",bid,rh))
    c.execute("INSERT OR IGNORE INTO dim_movie(movie_id,title,release_year,genres_json,is_deleted,effective_from,is_current,version,source_file,batch_id,record_hash) SELECT DISTINCT e.content_id,'Unknown movie '||e.content_id,NULL,'[]',0,'1900-01-01T00:00:00+00:00',1,1,'inferred','inferred',NULL FROM silver_events e LEFT JOIN dim_movie d ON d.movie_id=e.content_id AND d.is_current=1 WHERE e.content_id IS NOT NULL AND d.movie_id IS NULL")
    c.commit()

def load_static(c):
    for table in ("link","genome_tags","genome_scores"):
        path=LAKE/"landing"/f"{table}.csv"; bid=batch_id(table,"snapshot","v1")
        if c.execute("SELECT 1 FROM control WHERE batch_id=? AND status='SUCCESS'",(bid,)).fetchone(): continue
        rows=0; bronze_chunk=[]; ingested=NOW()
        with open(path,encoding="utf-8-sig",newline="",errors="replace") as f:
            for r in csv.DictReader(f):
                rows+=1; rh=digest(r)
                bronze_chunk.append((table,"movielens",bid,ingested,rows,rh,json.dumps(r,ensure_ascii=False,separators=(",",":"))))
                if table=="genome_scores":
                    try: c.execute("INSERT OR IGNORE INTO fact_genome VALUES(?,?,?,?,?,?)",(int(r['movieId']),int(r['tagId']),float(r['relevance']),table+".csv",bid,rh))
                    except Exception as e: c.execute("INSERT INTO quarantine VALUES(?,?,?,?,?,?)",(bid,table+".csv",rows,"GENOME_INVALID",str(e),json.dumps(r)))
                elif table=="genome_tags":
                    c.execute("INSERT OR IGNORE INTO dim_genome_tag VALUES(?,?,?,?,?)",(int(r["tagId"]),r["tag"],table+".csv",bid,rh))
                else:
                    imdb=int(r["imdbId"]) if r.get("imdbId") else None; tmdb=int(r["tmdbId"]) if r.get("tmdbId") else None
                    c.execute("INSERT OR IGNORE INTO dim_movie_link VALUES(?,?,?,?,?,?,?,?)",(int(r["movieId"]),imdb,tmdb,("https://www.imdb.com/title/tt"+str(imdb).zfill(7)+"/") if imdb else None,("https://www.themoviedb.org/movie/"+str(tmdb)) if tmdb else None,table+".csv",bid,rh))
                if len(bronze_chunk)>=50000:
                    c.executemany("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",bronze_chunk); bronze_chunk=[]; c.commit()
        if bronze_chunk: c.executemany("INSERT OR IGNORE INTO bronze VALUES(?,?,?,?,?,?,?)",bronze_chunk)
        if rows!=EXPECTED_ROWS[table]: raise RuntimeError(f"Landing reconciliation failed for {table}: expected {EXPECTED_ROWS[table]}, read {rows}")
        c.execute("INSERT OR REPLACE INTO control(batch_id,source_file,source_system,status,rows_read,rows_written,checksum,started_at,finished_at) VALUES(?,?,?,'SUCCESS',?,?,?,?,?)",(bid,table+".csv","movielens",rows,rows,sha_file(path),NOW(),NOW()))
    c.execute("INSERT OR IGNORE INTO dim_user(user_id,first_seen,source_file,batch_id) SELECT party_id,MIN(event_time_utc),'rating/tag','incremental' FROM silver_events WHERE party_id IS NOT NULL GROUP BY party_id")
    c.execute("INSERT INTO fact_interaction SELECT e.event_id,u.user_sk,d.movie_sk,e.party_id,e.content_id,e.event_type,e.event_value,e.event_time_utc,e.source_file,e.batch_id,e.record_hash,e.event_text FROM silver_events e LEFT JOIN dim_user u ON u.user_id=e.party_id LEFT JOIN dim_movie d ON d.movie_id=e.content_id AND e.event_time_utc>=d.effective_from AND (d.effective_to IS NULL OR e.event_time_utc<d.effective_to) WHERE 1 ON CONFLICT(event_id) DO UPDATE SET user_sk=excluded.user_sk,movie_sk=excluded.movie_sk WHERE fact_interaction.movie_sk IS NULL OR fact_interaction.movie_sk<>excluded.movie_sk")
    c.commit()

def report(c):
    out=LAKE/"reports"; out.mkdir(exist_ok=True)
    queries={
      "top_movies.csv":"SELECT f.content_id movie_id, d.title,COUNT(*) ratings,ROUND(AVG(f.event_value)/20.0,4) avg_rating,ROUND(AVG((f.event_value/20.0)*(f.event_value/20.0))-AVG(f.event_value/20.0)*AVG(f.event_value/20.0),4) variance FROM fact_interaction f JOIN dim_movie d ON d.movie_id=f.content_id WHERE f.event_type='RATING' AND d.is_current=1 AND d.is_deleted=0 GROUP BY f.content_id HAVING COUNT(*)>=100 ORDER BY avg_rating DESC,ratings DESC",
      "genre_summary.csv":"SELECT g.genre,COUNT(*) interactions,ROUND(AVG(f.event_value)/20.0,4) mean_rating,ROUND(AVG((f.event_value/20.0)*(f.event_value/20.0))-AVG(f.event_value/20.0)*AVG(f.event_value/20.0),4) variance FROM fact_interaction f JOIN dim_movie d ON d.movie_sk=f.movie_sk JOIN json_each(d.genres_json) g WHERE f.event_type='RATING' GROUP BY g.genre ORDER BY variance DESC",
      "rating_by_year.csv":"SELECT d.release_year,COUNT(*) ratings,ROUND(AVG(f.event_value)/20.0,4) avg_rating FROM fact_interaction f JOIN dim_movie d ON d.movie_sk=f.movie_sk WHERE f.event_type='RATING' AND d.release_year IS NOT NULL GROUP BY d.release_year ORDER BY d.release_year",
      "rating_by_event_month.csv":"SELECT substr(event_time_utc,1,7) month,COUNT(*) ratings,ROUND(AVG(event_value)/20.0,4) avg_rating FROM fact_interaction WHERE event_type='RATING' GROUP BY month ORDER BY month",
      "hidden_gems.csv":"SELECT d.movie_id,d.title,COUNT(f.event_id) ratings,ROUND(AVG(f.event_value)/20.0,4) avg_rating,l.imdb_url,l.tmdb_url FROM dim_movie d JOIN fact_interaction f ON f.movie_sk=d.movie_sk LEFT JOIN dim_movie_link l ON l.movie_id=d.movie_id WHERE f.event_type='RATING' AND d.is_current=1 AND d.is_deleted=0 GROUP BY d.movie_id HAVING ratings BETWEEN 10 AND 99 AND avg_rating>=4.0 ORDER BY avg_rating DESC,ratings DESC"
    }
    for filename,q in queries.items():
        cur=c.execute(q); rows=cur.fetchall()
        with open(out/filename,"w",newline="",encoding="utf-8") as f:
            w=csv.writer(f); w.writerow([x[0] for x in cur.description]); w.writerows(rows)
    movie_rating={mid:(n,avg) for mid,n,avg in c.execute("SELECT content_id,COUNT(*),AVG(event_value)/20.0 FROM fact_interaction WHERE event_type='RATING' GROUP BY content_id")}
    tag_movies={}; tag_events=Counter()
    for (payload,) in c.execute("SELECT payload FROM bronze WHERE source_file='tag'"):
        item=json.loads(payload); raw=item.get("tag") or ""
        norm=" ".join(unicodedata.normalize("NFKC",raw).casefold().split()).strip(string.punctuation+" \t\r\n")
        norm=re.sub(r"\s+"," ",norm)
        if not norm: continue
        if norm in ("scifi","sci fi","sci-fi"): norm="sci-fi"
        mid=int(item["movieId"]); tag_events[norm]+=1; tag_movies.setdefault(norm,set()).add(mid)
    with open(out/"popular_tags.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["normalized_tag","tag_events","distinct_movies"])
        for tag,n in tag_events.most_common(): w.writerow([tag,n,len(tag_movies[tag])])
    with open(out/"tag_rating_association.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        rated_values=[avg for _,avg in movie_rating.values()]; n_movies=len(rated_values); sum_y=sum(rated_values); sum_y2=sum(y*y for y in rated_values)
        global_mean=sum_y/n_movies if n_movies else None
        vals=[]
        for tag,movies in tag_movies.items():
            avgs=[movie_rating[mid][1] for mid in movies if mid in movie_rating]
            k=len(avgs)
            if avgs:
                sum_xy=sum(avgs); var_x=k*(n_movies-k)/n_movies if n_movies else 0; var_y=sum_y2-sum_y*sum_y/n_movies if n_movies else 0
                correlation=(sum_xy-k*sum_y/n_movies)/math.sqrt(var_x*var_y) if var_x>0 and var_y>0 else None
                vals.append((tag,k,sum(avgs)/k,global_mean,sum(avgs)/k-global_mean,correlation))
        w.writerow(["normalized_tag","rated_movies_with_tag","mean_movie_rating","global_mean_rating","difference_from_global","pearson_r_tag_presence_vs_movie_mean"])
        w.writerows(sorted(vals,key=lambda x:(-x[1],x[0])))
    with open(out/"genome_coverage.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["active_catalog_movies","genome_covered_movies","coverage_pct"])
        active=c.execute("SELECT COUNT(*) FROM dim_movie WHERE is_current=1 AND is_deleted=0").fetchone()[0]
        covered=c.execute("SELECT COUNT(DISTINCT movie_id) FROM fact_genome").fetchone()[0]
        w.writerow([active,covered,round(100*covered/active,4) if active else 0])
    with open(out/"genome_top_descriptors.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["movie_id","title","tag","relevance"])
        cur=c.execute("SELECT g.movie_id,m.title,t.tag,g.relevance FROM fact_genome g JOIN dim_genome_tag t USING(tag_id) LEFT JOIN dim_movie m ON m.movie_id=g.movie_id WHERE g.relevance>=0.9 ORDER BY g.relevance DESC LIMIT 500")
        w.writerows(cur.fetchall())
    with open(out/"genome_action_group.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["genre_group","genome_tag","movies_with_score","mean_relevance"])
        cur=c.execute("SELECT 'Action',t.tag,COUNT(DISTINCT g.movie_id),ROUND(AVG(g.relevance),4) FROM fact_genome g JOIN dim_genome_tag t USING(tag_id) JOIN dim_movie m ON m.movie_id=g.movie_id JOIN json_each(m.genres_json) genre WHERE genre.value='Action' AND m.is_current=1 AND m.is_deleted=0 GROUP BY t.tag ORDER BY AVG(g.relevance) DESC LIMIT 20")
        w.writerows(cur.fetchall())
    dq_batch="GOLD-QUALITY-CURRENT"; c.execute("DELETE FROM dq_results WHERE batch_id=?",(dq_batch,))
    checks=[
      ("rating_movie_fk","BLOCKING","SELECT COUNT(*) FROM fact_interaction f LEFT JOIN dim_movie d ON d.movie_id=f.content_id WHERE f.event_type='RATING' AND d.movie_id IS NULL"),
      ("genome_movie_fk","WARNING","SELECT COUNT(*) FROM fact_genome g LEFT JOIN silver_movie m ON m.movie_id=g.movie_id WHERE m.movie_id IS NULL"),
      ("genome_tag_fk","BLOCKING","SELECT COUNT(*) FROM fact_genome g LEFT JOIN dim_genome_tag t ON t.tag_id=g.tag_id WHERE t.tag_id IS NULL"),
      ("catalog_title_missing_year","WARNING","SELECT COUNT(*) FROM silver_movie WHERE release_year IS NULL"),
      ("catalog_no_genres_sentinel","WARNING","SELECT COUNT(*) FROM bronze WHERE source_file='movie' AND json_extract(payload,'$.genres')='(no genres listed)'"),
      ("link_missing_external_id","WARNING","SELECT COUNT(*) FROM dim_movie_link WHERE imdb_id IS NULL OR tmdb_id IS NULL")]
    for rule,severity,query in checks:
        observed=c.execute(query).fetchone()[0]; c.execute("INSERT INTO dq_results VALUES(?,?,?,?,?,?,?)",(dq_batch,rule,severity,int(observed==0),observed,0,"count of records violating rule"))
    c.commit()
    with open(out/"batch_audit.csv","w",newline="",encoding="utf-8") as f:
        cur=c.execute("SELECT batch_id,source_file,source_system,event_from,event_to,status,rows_read,rows_written,rows_quarantined,checksum,watermark,started_at,finished_at FROM control ORDER BY started_at"); w=csv.writer(f); w.writerow([x[0] for x in cur.description]); w.writerows(cur.fetchall())
    with open(out/"dq_results.csv","w",newline="",encoding="utf-8") as f:
        cur=c.execute("SELECT batch_id,rule_id,severity,passed,observed,threshold,detail FROM dq_results ORDER BY batch_id,rule_id"); w=csv.writer(f); w.writerow([x[0] for x in cur.description]); w.writerows(cur.fetchall())
    with open(out/"quarantine.csv","w",newline="",encoding="utf-8") as f:
        cur=c.execute("SELECT batch_id,source_file,row_number,error_code,reason,payload FROM quarantine ORDER BY batch_id,source_file,row_number"); w=csv.writer(f); w.writerow([x[0] for x in cur.description]); w.writerows(cur.fetchall())
    assert_no_blocking_failures(c)
    print(f"Reports written to {out}")

def run(args):
    init(); c=connect(); synthesize_movie_changes(c)
    for name in ("rating","tag"):
        cutoff=args.as_of
        ingest_event_years(c,name,cutoff,args.chunk_size)
        c.commit()
    assert_no_blocking_failures(c)
    movies_to_gold(c); load_static(c); assert_no_blocking_failures(c); report(c); c.close()

def main():
    p=argparse.ArgumentParser(description=__doc__); sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("init"); r=sub.add_parser("run"); r.add_argument("--chunk-size",type=int,default=CONFIG["processing"]["default_chunk_size"]); r.add_argument("--as-of",help="UTC cutoff YYYY-MM-DD")
    sub.add_parser("report"); args=p.parse_args()
    if args.cmd=="init": init()
    elif args.cmd=="run": run(args)
    else:
        with connect() as c: report(c)
if __name__=="__main__": main()
