"""Create a lightweight, dependency-free preview export for the delivered notebook."""
import html,json,csv
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
nb=json.loads((ROOT/"notebooks"/"movie_analytics.ipynb").read_text(encoding="utf-8"))
parts=["<!doctype html><html><head><meta charset='utf-8'><title>CineInsight MovieLens Analytics</title><style>body{font:16px/1.6 system-ui,sans-serif;max-width:960px;margin:40px auto;padding:0 24px;color:#172033}h1,h2{color:#183153}pre{background:#f3f5f8;padding:16px;overflow:auto;border-radius:8px}code{font-family:ui-monospace,monospace} .note{padding:12px 16px;background:#eef5ff;border-left:4px solid #3478c9}</style></head><body><h1>CineInsight MovieLens 20M: profiling and analytics</h1>"]
profile_path=ROOT/"lakehouse"/"reports"/"profile.json"
if profile_path.exists():
    profile=json.loads(profile_path.read_text(encoding="utf-8")); parts.append("<h2>Observed source profile</h2><table border='1' cellpadding='8' cellspacing='0'><tr><th>File</th><th>Rows</th><th>Empty cells</th></tr>")
    for x in profile: parts.append(f"<tr><td>{html.escape(x['file'])}</td><td>{x['rows']:,}</td><td>{x['empty_cells']:,}</td></tr>")
    rating=next((x for x in profile if x['file']=='rating.csv'),None)
    if rating: parts.append(f"<p>Rating values: <code>{html.escape(json.dumps(rating.get('rating_distribution',{})))}</code></p><p>Event range: {html.escape(rating.get('event_time_min',''))} to {html.escape(rating.get('event_time_max',''))}. Exact duplicate user/movie pairs: {rating.get('duplicate_user_movie_pairs')}; ordering check: {rating.get('user_movie_key_order_nondecreasing')}.</p><p>Rating skew indicators: max per user {rating.get('user_rating_max'):,} versus median {rating.get('user_rating_median'):,}; highest-volume movie has {rating.get('top_movie_ratings',[[None,0]])[0][1]:,} ratings.</p>")
    movie=next((x for x in profile if x['file']=='movie.csv'),None); tag=next((x for x in profile if x['file']=='tag.csv'),None); link=next((x for x in profile if x['file']=='link.csv'),None)
    if movie: parts.append(f"<p>Movie catalog quality: {movie.get('year_suffix_distribution',{}).get('missing_year',0)} titles lack a terminal year; {movie.get('column_nonempty_counts',{}).get('no_genres_sentinel',0)} use the explicit no-genre sentinel.</p>")
    if tag: parts.append(f"<p>Tag quality: {tag.get('column_nonempty_counts',{}).get('empty_or_whitespace_tag',0)} blank/whitespace tags.</p>")
    if link: parts.append(f"<p>Link quality: {link['rows']-link.get('column_nonempty_counts',{}).get('tmdbId',0)} missing TMDb identifiers.</p>")
else: parts.append("<p class='note'>Run <code>src/profile.py</code> to populate observed source counts.</p>")
for cell in nb['cells']:
    source=''.join(cell.get('source',[]))
    if cell['cell_type']=='markdown':
        for para in source.split('\n\n'):
            p=para.strip()
            if not p: continue
            if p.startswith('# '): parts.append('<h1>'+html.escape(p[2:])+'</h1>')
            elif p.startswith('## '): parts.append('<h2>'+html.escape(p[3:])+'</h2>')
            else: parts.append('<p>'+html.escape(p).replace('\n','<br>')+'</p>')
    # The HTML is a reading preview; runnable code stays in the .ipynb.
report_names=['top_movies.csv','genre_summary.csv','rating_by_year.csv','rating_by_event_month.csv','popular_tags.csv','tag_rating_association.csv','hidden_gems.csv','genome_coverage.csv','genome_action_group.csv']
available=[]
for name in report_names:
    path=ROOT/'lakehouse'/'reports'/name
    if not path.exists(): continue
    available.append(name)
    with path.open(encoding='utf-8',newline='') as f:
        rows=list(csv.reader(f))
    if not rows: continue
    parts.append('<h2>'+html.escape(name.replace('_',' ').removesuffix('.csv').title())+'</h2><table border="1" cellpadding="6" cellspacing="0"><thead><tr>'+''.join('<th>'+html.escape(c)+'</th>' for c in rows[0])+'</tr></thead><tbody>')
    for row in rows[1:11]: parts.append('<tr>'+''.join('<td>'+html.escape(v)+'</td>' for v in row)+'</tr>')
    parts.append('</tbody></table>')
if not available:
    parts.append("<p class='note'>No generated Gold reports were found in <code>lakehouse/reports</code>. Run the pipeline, then regenerate this HTML preview to include measured results.</p>")
parts.append("</body></html>")
(ROOT/"notebooks"/"movie_analytics.html").write_text('\n'.join(parts),encoding="utf-8")
print(ROOT/"notebooks"/"movie_analytics.html")
