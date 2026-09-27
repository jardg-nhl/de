"""Streaming, bounded-memory dataset profile. Writes lakehouse/reports/profile.json."""
import csv, json
from collections import Counter
from pathlib import Path
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parents[2]; CONFIG=json.loads((ROOT/"config"/"pipeline.json").read_text(encoding="utf-8")); DATA=ROOT/CONFIG["paths"]["source"]; OUT=ROOT/CONFIG["paths"]["lakehouse"]/"reports"/"profile.json"
SPECS={"rating.csv":["userId","movieId","rating","timestamp"],"tag.csv":["userId","movieId","tag","timestamp"],"movie.csv":["movieId","title","genres"],"link.csv":["movieId","imdbId","tmdbId"],"genome_scores.csv":["movieId","tagId","relevance"],"genome_tags.csv":["tagId","tag"]}
def profile(path):
    rows=nulls=0; counts=Counter(); ratings=Counter(); years=Counter(); per_user=Counter(); ids=Counter(); tsmin=tsmax=None; dups=0; last_key=None; sorted_keys=True; full_dups=0
    with open(path,encoding="utf-8-sig",newline="",errors="replace") as f:
        reader=csv.DictReader(f)
        for r in reader:
            rows+=1
            for k,v in r.items():
                if v is None or not v.strip(): nulls+=1
                if v is not None and v.strip(): counts[k]+=1
            if path.name=="rating.csv":
                try:
                    ratings[r["rating"]]+=1; per_user[r["userId"]]+=1; ids[r["movieId"]]+=1
                    key=(int(r["userId"]),int(r["movieId"]))
                    if last_key is not None and key<last_key: sorted_keys=False
                    if last_key==key: full_dups+=1
                    last_key=key
                    raw=r["timestamp"]
                    try: t=int(raw)
                    except ValueError: t=int(datetime.strptime(raw,"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
                    tsmin=min(tsmin,t) if tsmin is not None else t; tsmax=max(tsmax,t) if tsmax is not None else t
                except Exception: pass
            elif path.name=="movie.csv":
                import re
                m=re.search(r"\((\d{4})\)\s*$",r.get("title", ""))
                years[m.group(1) if m else "missing_year"]+=1
                if r.get("genres")=="(no genres listed)": counts["no_genres_sentinel"]+=1
            elif path.name=="tag.csv":
                if not (r.get("tag") or "").strip(): counts["empty_or_whitespace_tag"]+=1
    out={"file":path.name,"rows":rows,"empty_cells":nulls,"column_nonempty_counts":dict(counts)}
    if ratings:
        out.update(rating_distribution=dict(sorted(ratings.items(),key=lambda x:float(x[0]))),unique_users=len(per_user),unique_movies=len(ids),top_user_ratings=per_user.most_common(10),top_movie_ratings=ids.most_common(10),user_rating_max=max(per_user.values()),user_rating_median=sorted(per_user.values())[len(per_user)//2],event_time_min=datetime.fromtimestamp(tsmin,timezone.utc).isoformat(),event_time_max=datetime.fromtimestamp(tsmax,timezone.utc).isoformat(),user_movie_key_order_nondecreasing=sorted_keys,duplicate_user_movie_pairs=full_dups if sorted_keys else None)
    if years: out["year_suffix_distribution"]=dict(years)
    return out
def main():
    results=[profile(DATA/name) for name in SPECS]
    OUT.parent.mkdir(parents=True,exist_ok=True); OUT.write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding="utf-8"); print(json.dumps(results,ensure_ascii=False,indent=2))
if __name__=="__main__":main()
