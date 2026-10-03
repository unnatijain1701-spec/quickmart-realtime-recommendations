"""
m5_dashboard.py - Milestone 5: the business dashboard (Plotly Dash).

Reads what the pipeline has stored in MongoDB and refreshes every few seconds.
Run:   python m5_dashboard.py      then open  http://127.0.0.1:8050
"""
from datetime import timezone

import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from plotly.subplots import make_subplots

import config
from dashboard_data import FUNNEL_STAGES, DataLayer

GREEN, AMBER, GREY, RED, TEAL, BROWN, INK = "#14532D", "#F59E0B", "#6B7280", "#B91C1C", "#0F766E", "#B45309", "#1B1F23"
GROUP_COLOR = {"treatment": GREEN, "control": GREY}
RULE_COLOR = {"cart_complement": GREEN, "offer": TEAL, "trending": BROWN}
RULE_LABEL = {"cart_complement": "Cart complement", "offer": "Offer", "trending": "Trending"}
FONT = dict(family="Calibri, Arial, sans-serif", color=INK)


# ------------------------------------------------------------------ helpers
def local(dt):
    return dt.replace(tzinfo=timezone.utc).astimezone()


def hhmm(dt):
    return local(dt).strftime("%H:%M")


def minute_label(s):
    """'2026-10-02T18:29' (UTC) -> local 'HH:MM'"""
    from datetime import datetime
    return local(datetime.strptime(s, "%Y-%m-%dT%H:%M")).strftime("%H:%M")


def style(fig, title, height=300):
    fig.update_layout(title=dict(text=title, font=dict(size=16, color=GREEN)), template="plotly_white", font=FONT,
                      height=height, margin=dict(l=45, r=15, t=50, b=40), legend=dict(orientation="h", y=-0.2),
                      plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF")
    return fig


def empty_fig(title, note="No data in this window yet", height=300):
    fig = go.Figure()
    fig.add_annotation(text=note, showarrow=False, font=dict(size=14, color=GREY), x=0.5, y=0.5, xref="paper", yref="paper")
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return style(fig, title, height)


def pct(n, d):
    return f"{100 * n / d:.1f}%" if d else "n/a"


# ------------------------------------------------------------------ KPI cards
def kpi(label, value, sub="", badge=None):
    """badge = (text, 'ok' | 'warn' | 'bad') shown next to the value."""
    val = value if not badge else [value, html.Span(badge[0], className="bdg bdg-" + badge[1])]
    return html.Div([html.Div(label, className="kpi-label"), html.Div(val, className="kpi-value"),
                     html.Div(sub, className="kpi-sub")], className="kpi")


def build_kpis(d):
    m = d["metrics"] or []
    cur = m[-1] if m else None
    done = m[:-1] if len(m) > 1 else m          # completed minutes only (the current one is still filling)
    last5 = done[-5:]
    ev5, late5 = sum(x["events"] for x in last5), sum(x["late_events"] for x in last5)
    recs5 = sum(x["recs"] for x in last5)
    shown = clicked = added = 0
    for r in d["rec"] or []:
        shown += r["n"] if r["t"] == "rec_shown" else 0
        clicked += r["n"] if r["t"] == "rec_clicked" else 0
        added += r["n"] if r["t"] == "rec_added_to_cart" else 0
    if cur and cur["recs"]:
        avg_s, p95_s = cur["rec_latency_avg_ms"] / 1000, cur["rec_latency_p95_ms"] / 1000
        badge = ("Fast", "ok") if p95_s < 1 else (("Slow", "warn") if p95_s <= 2 else ("Too slow", "bad"))
        speed = kpi("How fast we suggest", f"{avg_s:.2f} seconds",
                    "Time from the customer's tap to our suggestion. "
                    + (f"Even the slowest few took only {p95_s:.2f} seconds. " if p95_s < 1 else f"The slowest few took {p95_s:.2f} seconds. ")
                    + "Goal: under 1 second.", badge)
    else:
        speed = kpi("How fast we suggest", "n/a", "No suggestions yet this minute. Goal: under 1 second.")
    return [
        kpi("Customer actions per second", f"{d['eps']:.1f}" if d["eps"] is not None else "n/a", "How busy the shop is right now"),
        speed,
        kpi("Suggestions made per minute", f"{recs5 / max(len(last5), 1):.0f}" if last5 else "n/a", "How much work the system is doing"),
        kpi("Recommendation CTR", pct(clicked, shown), f"{clicked} clicks / {shown} shown"),
        kpi("Add-to-cart rate", pct(added, shown), f"{added} added / {shown} shown"),
        kpi("Messages that arrived late", pct(late5, ev5), "Phones that lost signal. A few percent is normal"),
    ]


# ------------------------------------------------------------------ charts
def fig_funnel(d):
    f = d["funnel"]
    if not f or not f.get("app_open"):
        return empty_fig("Customer funnel (sessions)")
    labels = [lab for t, lab in FUNNEL_STAGES]
    counts = [f[t] for t, _ in FUNNEL_STAGES]
    fig = go.Figure(go.Funnel(y=labels, x=counts, textinfo="value+percent initial",
                              marker=dict(color=[GREEN, "#1F7A44", "#2F9E5B", AMBER, BROWN])))
    return style(fig, f"Customer funnel: sessions that reached each step (last {d['window_min']} min)", 320)


def fig_health(d):
    m = d["metrics"]
    if not m:
        return empty_fig("Streaming health")
    x = [hhmm(b["bucket_start"]) for b in m]
    now_naive = d["now"].replace(tzinfo=None)
    secs = [max(1.0, min(config.TUMBLING_SEC, (now_naive - b["bucket_start"]).total_seconds())) for b in m]   # partial current minute
    cap = 2.5
    vals = [b["rec_latency_p95_ms"] / 1000 if b["rec_latency_p95_ms"] else None for b in m]
    shown = [None if v is None else min(v, cap) for v in vals]
    clipped = [v is not None and v > cap for v in vals]
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.2,
                        subplot_titles=("Events per second", "How long suggestions took: the slowest 5% (seconds)"))
    fig.update_annotations(font=dict(size=12, color=GREY))          # subplot titles only (the zone labels are added after this)
    fig.add_trace(go.Scatter(x=x, y=[b["events"] / s_ for b, s_ in zip(m, secs)], mode="lines+markers",
                             line=dict(color=GREEN), showlegend=False), row=1, col=1)
    fig.add_hrect(y0=0, y1=1, fillcolor="#DCFCE7", opacity=0.9, line_width=0, layer="below", row=2, col=1, exclude_empty_subplots=False)
    fig.add_hrect(y0=2, y1=cap, fillcolor="#FEE2E2", opacity=0.9, line_width=0, layer="below", row=2, col=1, exclude_empty_subplots=False)
    fig.add_trace(go.Scatter(x=x, y=shown, mode="lines+markers", line=dict(color=AMBER), showlegend=False, customdata=vals,
                             marker=dict(symbol=["triangle-up" if c else "circle" for c in clipped], size=[12 if c else 6 for c in clipped],
                                         color=[RED if c else AMBER for c in clipped]),
                             hovertemplate="%{x}: %{customdata:.2f} seconds<extra></extra>"), row=2, col=1)
    fig.update_yaxes(range=[0, cap], title_text="seconds", row=2, col=1)
    fig.update_xaxes(type="category")                       # minutes with no data show as gaps, not skipped
    fig.add_annotation(text="Fast enough: under 1 second", x=0.01, y=0.5, xref="x2 domain", yref="y2", showarrow=False,
                       xanchor="left", font=dict(size=11, color="#166534"))
    fig.add_annotation(text="Too slow: over 2 seconds", x=0.01, y=2.25, xref="x2 domain", yref="y2", showarrow=False,
                       xanchor="left", font=dict(size=11, color=RED))
    if any(clipped):
        fig.add_annotation(text="Red triangle = longer than 2.5 seconds (hover to see how long)", x=0.99, y=1.5, xref="x2 domain", yref="y2",
                           showarrow=False, xanchor="right", font=dict(size=11, color=RED))
    return style(fig, "Streaming health, per minute", 420)


def fig_late(d):
    title = "Late messages: when it happened vs when we heard"
    late = d["late"]
    if not late or not (late["by_event"] or late["by_proc"]):
        return empty_fig(title, "No late messages yet (they arrive 30 seconds to 5 minutes late)")
    from datetime import datetime, timedelta
    mins = sorted(set(late["by_event"]) | set(late["by_proc"]))
    recent_from = (d["now"].replace(tzinfo=None) - timedelta(minutes=5)).replace(second=0, microsecond=0)
    recent = [datetime.strptime(k, "%Y-%m-%dT%H:%M") >= recent_from for k in mins]      # still arriving
    x = [minute_label(k) for k in mins]
    fig = go.Figure()
    fig.add_bar(x=x, y=[late["by_event"].get(k, 0) for k in mins], name="When the customer did it",
                marker_color=["#D1D5DB" if r else GREEN for r in recent])
    fig.add_bar(x=x, y=[late["by_proc"].get(k, 0) for k in mins], name="When we heard about it",
                marker_color=["#E5E7EB" if r else AMBER for r in recent])
    fig.update_layout(barmode="group")
    fig.update_yaxes(title_text="Number of late messages")
    if any(recent):
        fig.add_annotation(text="Grey = last 5 minutes, still arriving", xref="paper", yref="paper", x=1, y=1.0, xanchor="right",
                           showarrow=False, font=dict(size=11, color=GREY))
    return style(fig, title, 340)


def fig_rules_volume(d):
    m = d["metrics"]
    if not m or not any(b["recs"] for b in m):
        return empty_fig("Recommendations shown, by rule")
    x = [hhmm(b["bucket_start"]) for b in m]
    fig = go.Figure()
    for rule in ("cart_complement", "offer", "trending"):
        fig.add_bar(x=x, y=[b["recs_by_rule"].get(rule, 0) for b in m], name=RULE_LABEL[rule], marker_color=RULE_COLOR[rule])
    fig.update_layout(barmode="stack")
    return style(fig, "Recommendations produced, by rule (per minute)", 320)


def build_rule_table(d):
    stats = {}
    for r in d["rec"] or []:
        s = stats.setdefault(r["rule"], {"rec_shown": 0, "rec_clicked": 0, "rec_added_to_cart": 0})
        s[r["t"]] += r["n"]
    head = html.Tr([html.Th(h) for h in ("Rule", "Shown", "Clicked", "CTR", "Added to cart", "Add rate")])
    rows = []
    for rule in ("cart_complement", "offer", "trending"):
        s = stats.get(rule, {"rec_shown": 0, "rec_clicked": 0, "rec_added_to_cart": 0})
        rows.append(html.Tr([html.Td(RULE_LABEL[rule]), html.Td(s["rec_shown"]), html.Td(s["rec_clicked"]),
                             html.Td(pct(s["rec_clicked"], s["rec_shown"])), html.Td(s["rec_added_to_cart"]),
                             html.Td(pct(s["rec_added_to_cart"], s["rec_shown"]))]))
    return html.Table([head] + rows, className="tbl")


def wilson(k, n, z=1.96):
    if n == 0:
        return 0.0, 0.0, 0.0
    p = k / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / (1 + z * z / n)
    return p, max(0.0, centre - half), min(1.0, centre + half)


def fig_business(d):
    b = d["business"]
    if not b or not any(v["sessions"] for v in b["sessions"].values()):
        return empty_fig("Live vs stale recommendations")
    groups = ["control", "treatment"]
    names = ["Stale (control)", "Live (treatment)"]
    sess = [b["sessions"].get(g, {"sessions": 0, "converted": 0}) for g in groups]
    ords = [b["orders"].get(g, {"n": 0, "value": 0, "sumsq": 0}) for g in groups]
    ci = [wilson(s["converted"], s["sessions"]) for s in sess]
    conv = [100 * c[0] for c in ci]
    up = [100 * (c[2] - c[0]) for c in ci]
    dn = [100 * (c[0] - c[1]) for c in ci]
    aov, aov_ci = [], []
    for o in ords:
        n = o["n"]
        mean = o["value"] / n if n else 0
        sd = max(0.0, o["sumsq"] / n - mean * mean) ** 0.5 if n else 0
        aov.append(mean)
        aov_ci.append(1.96 * sd / n ** 0.5 if n > 1 else 0)
    labels = [f"n={s['sessions']} sessions" for s in sess]
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Conversion rate (%) with 95% interval", "Average order value (Rs) with 95% interval"))
    fig.add_trace(go.Bar(x=names, y=conv, marker_color=[GREY, GREEN], showlegend=False, error_y=dict(type="data", symmetric=False, array=up, arrayminus=dn),
                         text=[f"{v:.1f}%<br>{l}" for v, l in zip(conv, labels)], textposition="inside", insidetextanchor="start",
                         textfont=dict(color="#FFFFFF")), row=1, col=1)
    fig.add_trace(go.Bar(x=names, y=aov, marker_color=[GREY, GREEN], showlegend=False, error_y=dict(type="data", array=aov_ci),
                         text=[f"{v:,.0f}<br>n={o['n']} orders" for v, o in zip(aov, ords)], textposition="inside", insidetextanchor="start",
                         textfont=dict(color="#FFFFFF")), row=1, col=2)
    fig.update_yaxes(rangemode="tozero")
    fig.update_annotations(font=dict(size=12, color=GREY))
    overlap = max(ci[0][1], ci[1][1]) <= min(ci[0][2], ci[1][2])
    verdict = ("The intervals overlap: the gap is not clearly beyond random noise." if overlap
               else "The intervals do not overlap: the gap is unlikely to be random noise.")
    style(fig, f"Business impact: live vs stale (last {d['window_min']} min)", 360)
    fig.add_annotation(text=verdict, xref="paper", yref="paper", x=0.5, y=-0.24, showarrow=False, font=dict(size=12, color=GREY))
    fig.update_layout(margin=dict(l=45, r=15, t=60, b=80))
    return fig


def fig_experiment(d):
    e = d.get("experiment")
    if not e:
        return empty_fig("Experiment: live vs stale recommendations", "No experiment yet. Run: python m6_experiment.py", 380)
    names = list(e["scenarios"])
    short = {"no_relevance": "None", "low": "Low", "base": "Base", "high": "High"}
    fig = make_subplots(rows=1, cols=3, subplot_titles=("Conversion rate", "Average order value", "Revenue per session"))
    for col, key in enumerate(("conversion", "aov", "rps"), 1):
        u = [e["scenarios"][n]["live_vs_stale"][key] for n in names]
        labels = [f"{short.get(n, n)}<br>{100 * v['rel']:+.1f}%" for n, v in zip(names, u)]     # the number sits in the axis label, never on the bar
        fig.add_trace(go.Bar(x=labels, y=[100 * v["rel"] for v in u], marker_color=[GREY, "#2F9E5B", GREEN, GREEN],
                             error_y=dict(type="data", symmetric=False, array=[100 * (v["hi"] - v["rel"]) for v in u],
                                          arrayminus=[100 * (v["rel"] - v["lo"]) for v in u]), showlegend=False), row=1, col=col)
    fig.update_yaxes(ticksuffix="%", zeroline=True, zerolinewidth=2, zerolinecolor=INK)
    fig.update_xaxes(tickangle=0, title_text="How much customers care<br>about useful suggestions", title_font=dict(size=11, color=GREY))
    fig.update_annotations(font=dict(size=12, color=GREY))
    p = e["params"]
    style(fig, "Experiment: live vs stale recommendations (relative change)", 560)
    fig.add_annotation(xref="paper", yref="paper", x=0.5, y=-0.78, showarrow=False, font=dict(size=11, color=GREY), align="center",
                       text=(f"Average of {p['replications']} independent replications of {p['measured_sessions_per_replication']} measured sessions each;<br>"
                             f"bars show the 95% interval across replications.<br>"
                             f"Customer response is an assumption (low, base, high); simulated, not observed."))
    fig.update_layout(margin=dict(l=45, r=15, t=60, b=270))
    return fig


def fig_ratings(d):
    live, base = d["ratings"] or {}, d["baseline"] or {}
    cats = sorted(c for c in base if c)
    if not cats:
        return empty_fig("Average rating by category")
    fig = go.Figure()
    fig.add_bar(x=cats, y=[base[c] for c in cats], name="Historical average", marker_color="#9CA3AF")
    if live:
        fig.add_bar(x=cats, y=[live[c][0] if c in live else None for c in cats], name=f"Live ratings (last {d['window_min']} min)",
                    marker_color=GREEN, text=[f"n={live[c][1]}" if c in live else "" for c in cats], textposition="outside")
    fig.update_layout(barmode="group")
    fig.update_yaxes(range=[0, 5], title="rating")
    fig.update_xaxes(tickangle=-35)
    style(fig, "Average rating by category", 400)
    fig.update_layout(margin=dict(l=45, r=15, t=50, b=130), legend=dict(orientation="h", y=-0.55))
    return fig


def build_alerts(d):
    import re
    from datetime import datetime
    al = d["alerts"]
    if not al:
        return html.Div([html.Span("All clear", className="bdg bdg-ok bdg-big"),
                         html.Span("Nothing is wrong. A warning appears here if suggestions get slow or too many messages arrive late.", className="muted")])

    def when(a):
        try:
            return datetime.fromisoformat(a["bucket_start"]).astimezone().strftime("%H:%M")
        except Exception:
            return ""

    def human(ms):
        return f"{ms / 60000:.0f} minutes" if ms >= 60000 else (f"{ms / 1000:.0f} seconds" if ms >= 1000 else f"{ms:.0f} milliseconds")

    items = []
    for a in al:
        t = when(a)
        at = f" around {t}" if t else ""
        msg = a.get("message", "")
        if a.get("kind") == "processing_delay" and re.search(r"(\d+(?:\.\d+)?) ms", msg):
            ms = float(re.search(r"(\d+(?:\.\d+)?) ms", msg).group(1))
            text, hint = f"Suggestions were slow{at}: some waited about {human(ms)}.", "What to do: check that the processor (terminal B) is running."
        elif a.get("kind") == "late_share" and re.search(r"(\d+(?:\.\d+)?)%", msg):
            pct_ = re.search(r"(\d+(?:\.\d+)?)%", msg).group(1)
            text, hint = f"Many messages arrived late{at}: {pct_}% (the limit is 15%).", "What to do: usually phones that lost signal. Check the network if it keeps happening."
        else:
            text, hint = msg, ""
        items.append(html.Li([html.B(text), html.Div(hint, className="muted") if hint else ""]))
    return html.Ul(items, className="alerts")


# ------------------------------------------------------------------ the app
CSS = """
body{margin:0;background:#F1F5F2;font-family:Calibri,Arial,sans-serif;color:#1B1F23}
.hdr{background:#14532D;color:#fff;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
.hdr h1{margin:0;font-size:24px;font-family:Cambria,Georgia,serif}
.hdr .ctl{display:flex;align-items:center;gap:14px;font-size:14px}
.page{max-width:1400px;margin:0 auto;padding:16px}
.kpis{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:14px}
.kpis.k4{grid-template-columns:repeat(4,1fr)}
.bdg{font-size:12px;font-weight:700;padding:2px 9px;border-radius:999px;margin-left:10px;vertical-align:middle;font-family:Calibri,Arial,sans-serif}
.bdg-ok{background:#DCFCE7;color:#14532D}.bdg-warn{background:#FEF3C7;color:#92400E}.bdg-bad{background:#FEE2E2;color:#B91C1C}
.bdg-big{font-size:14px;padding:4px 12px;margin:0 10px 0 8px}
.purpose{background:#E7EEE9;color:#14532D;border-radius:10px;padding:8px 14px;margin-bottom:12px;font-size:14px;font-weight:600}
.kpi{background:#14532D;color:#fff;border-radius:12px;padding:12px 14px}
.kpi-label{font-size:12px;letter-spacing:.04em;text-transform:uppercase;color:#FDE68A}
.kpi-value{font-size:26px;font-weight:700;margin:4px 0;font-family:Cambria,Georgia,serif}
.kpi-sub{font-size:12px;color:#D1FAE5}
.row{display:grid;gap:14px;margin-bottom:14px}
.r2{grid-template-columns:1fr 1fr}.r3{grid-template-columns:1.1fr 1fr 1fr}
.card{background:#fff;border-radius:12px;padding:8px 10px;box-shadow:0 1px 3px rgba(0,0,0,.08);min-width:0}
.card h3{margin:8px 8px 6px;font-size:16px;color:#14532D;font-family:Cambria,Georgia,serif}
.tbl{width:100%;border-collapse:collapse;font-size:14px}
.tbl th{text-align:left;color:#6B7280;font-weight:600;padding:6px 8px;border-bottom:1px solid #E5E7EB}
.tbl td{padding:8px;border-bottom:1px solid #F3F4F6}
.muted{color:#6B7280;font-size:13px;padding:6px 8px}
.alerts{margin:6px 8px;padding-left:18px;font-size:14px}
.note{color:#6B7280;font-size:12px;padding:0 8px 8px}
.warn{color:#FDE68A;font-size:12px}
@media (max-width:1100px){.kpis,.kpis.k4{grid-template-columns:repeat(2,1fr)}.r2,.r3{grid-template-columns:1fr}}
"""


def build_engagement_kpis(d):
    shown = clicked = added = 0
    for r in d["rec"] or []:
        shown += r["n"] if r["t"] == "rec_shown" else 0
        clicked += r["n"] if r["t"] == "rec_clicked" else 0
        added += r["n"] if r["t"] == "rec_added_to_cart" else 0
    low = " (low sample)" if shown < 30 else ""
    return [kpi("Recommendations shown", f"{shown:,}", "displayed on a customer screen, in the window"),
            kpi("Recommendation CTR", pct(clicked, shown), f"{clicked} clicks / {shown} shown" + low),
            kpi("Add-to-cart rate", pct(added, shown), f"{added} added / {shown} shown" + low),
            kpi("Recommendations / min", f"{sum(x['recs'] for x in (d['metrics'] or [])[:-1][-5:]) / max(len((d['metrics'] or [])[:-1][-5:]), 1):.0f}" if d["metrics"] and len(d["metrics"]) > 1 else "n/a", "produced by the processor, last 5 minutes")]


def build_health_kpis(d):
    k = build_kpis(d)
    return [k[0], k[1], k[2], k[5]]


def build_business_kpis(d):
    b = d["business"]
    if not b:
        return [kpi("Customer visits", "n/a"), kpi("Conversion", "n/a"), kpi("Average order value", "n/a"), kpi("Revenue per session", "n/a")]
    sessions = sum(v["sessions"] for v in b["sessions"].values())
    converted = sum(v["converted"] for v in b["sessions"].values())
    orders = sum(v["n"] for v in b["orders"].values())
    value = sum(v["value"] for v in b["orders"].values())
    w = d["window_min"]
    return [kpi("Customer visits", f"{sessions:,}", f"How many times someone opened the app, last {w} min"),
            kpi("Conversion rate", pct(converted, sessions), f"{converted:,} sessions with an order"),
            kpi("Average order value", f"Rs {value / orders:,.0f}" if orders else "n/a", f"{orders:,} orders, after promotions"),
            kpi("Revenue per session", f"Rs {value / sessions:,.0f}" if sessions else "n/a", "conversion times order value")]


def build_quality_kpis(d):
    q, r = d["quality"], d["replaced"] or {"items": 0, "replaced": 0}
    live = sum(n for _, n in (d["ratings"] or {}).values())
    return [kpi("Products with a reliable rating", f"{q['reliable']} / {q['products']}", f"{config.RATING_MIN_COUNT} or more ratings each"),
            kpi(f"Poorly rated products (under {config.RATING_LOW_CUTOFF:g} stars)", f"{q['below']}", "these are never recommended as they stand"),
            kpi("Low-rated items replaced", f"{r['replaced']:,}", f"of {r['items']:,} recommended items, last {d['window_min']} min"),
            kpi("Live ratings received", f"{live:,}", f"in the last {d['window_min']} min")]


def build_low_rated_table(d):
    rows = d["quality"]["low_list"]
    head = html.Tr([html.Th(h) for h in ("Product", "Category", "Average", "Ratings")])
    body = [html.Tr([html.Td(f"{r['name']} ({r['brand']})"), html.Td(r["category"]), html.Td(f"{r['avg']:.2f}"), html.Td(r["n"])]) for r in rows]
    return html.Table([head] + body, className="tbl")


TABS = [("health", "Pipeline health", "For engineers and operators. Is the stream flowing, fast and on time?"),
        ("engagement", "Recommendation engagement", "For the product and merchandising team. Do customers act on what we show?"),
        ("business", "Business outcome", "For managers and executives. Does a live system earn more per shopping session?"),
        ("quality", "Quality guard rail", "For category managers. Are we recommending well-rated products?")]


def build_app(dl, refresh_ms=None):
    app = Dash(__name__, title="QuickMart live dashboard")
    app.index_string = app.index_string.replace("</head>", f"<style>{CSS}</style></head>")
    graph = lambda i: dcc.Graph(id=i, config={"displayModeBar": False})
    card = lambda *c, **kw: html.Div(list(c), className="card", **kw)
    purpose = lambda t: html.Div(t[2], className="purpose")
    tab_style = {"padding": "10px 16px", "fontWeight": "600", "color": "#6B7280", "backgroundColor": "#E7EEE9", "border": "0"}
    tab_sel = dict(tab_style, color="#FFFFFF", backgroundColor="#14532D")

    panes = {
        "health": [html.Div(id="kpis_health", className="kpis k4"),
                   html.Div([card(graph("health")),
                              card(graph("late"), html.Div("Green = when the customer did it. Amber = when we heard about it. Grey = the last 5 minutes, still arriving.", className="note"))], className="row r2"),
                   card(html.H3("Pipeline alerts"), html.Div(id="alerts"))],
        "engagement": [html.Div(id="kpis_engage", className="kpis k4"),
                       html.Div([card(graph("rules_volume")),
                                 card(html.H3("Rule performance"), html.Div(id="rule_table"),
                                      html.Div("Click-through is measured on the customer screen. Simulated customers respond in the experiment run (M6).", className="note"))], className="row r2")],
        "business": [html.Div(id="kpis_business", className="kpis k4"),
                     html.Div([card(graph("funnel")),
                               card(graph("business"), html.Div("Until the experiment run (M6), simulated customers do not react to recommendations, so any gap here is chance. The interval shows how big chance alone can be.", className="note"))], className="row r2"),
                     card(graph("experiment"), html.Div("Higher bar = live does better. The thin line is the maybe range.", className="note"))],
        "quality": [html.Div(id="kpis_quality", className="kpis k4"),
                    html.Div([card(graph("ratings"), html.Div("Grey = past ratings. Green = new ratings. 'n' = how many", className="note")),
                              card(html.H3("Lowest reliably rated products"), html.Div(id="low_table"),
                                   html.Div("The rating rule replaces these when another rule picks them.", className="note"))], className="row r2")],
    }
    app.layout = html.Div([
        html.Div([html.H1("QuickMart: live recommendation dashboard"),
                  html.Div([html.Span("Window:"),
                            dcc.Dropdown(id="window", options=[{"label": f"{m} minutes", "value": m} for m in (5, 15, 60)],
                                         value=config.DASH_DEFAULT_WINDOW_MIN, clearable=False, style={"width": "130px", "color": "#1B1F23"}),
                            html.Span(id="stamp"), html.Span(id="warn", className="warn")], className="ctl")], className="hdr"),
        html.Div([
            dcc.Tabs(id="tabs", value="health", style={"marginBottom": "12px"}, colors={"border": "#E5E7EB", "primary": "#14532D", "background": "#E7EEE9"},
                     children=[dcc.Tab(label=label, value=key, style=tab_style, selected_style=tab_sel) for key, label, _ in TABS]),
            *[html.Div([purpose(t)] + panes[t[0]], id="pane_" + t[0], style={"display": "block" if t[0] == "health" else "none"}) for t in TABS],
        ], className="page"),
        dcc.Interval(id="tick", interval=refresh_ms or config.DASH_REFRESH_MS),
    ])

    def show(tab):
        return tuple({"display": "block" if t[0] == tab else "none"} for t in TABS)

    def refresh(_n, window):
        d = dl.gather(window or config.DASH_DEFAULT_WINDOW_MIN)
        warn = ("  (some queries failed: " + "; ".join(d["errors"].values()) + ")") if d["errors"] else ""
        return (build_health_kpis(d), build_engagement_kpis(d), build_business_kpis(d), build_quality_kpis(d),
                fig_funnel(d), fig_health(d), fig_late(d), fig_rules_volume(d), build_rule_table(d),
                fig_business(d), fig_ratings(d), fig_experiment(d), build_alerts(d), build_low_rated_table(d),
                "updated " + hhmmss(d["now"]), warn)

    app.callback(Output("kpis_health", "children"), Output("kpis_engage", "children"), Output("kpis_business", "children"),
                 Output("kpis_quality", "children"), Output("funnel", "figure"), Output("health", "figure"), Output("late", "figure"),
                 Output("rules_volume", "figure"), Output("rule_table", "children"), Output("business", "figure"),
                 Output("ratings", "figure"), Output("experiment", "figure"), Output("alerts", "children"), Output("low_table", "children"),
                 Output("stamp", "children"), Output("warn", "children"),
                 Input("tick", "n_intervals"), Input("window", "value"))(refresh)
    app.callback(*[Output("pane_" + t[0], "style") for t in TABS], Input("tabs", "value"))(show)
    app.refresh, app.show = refresh, show          # exposed for tests
    return app


def hhmmss(dt):
    return local(dt).strftime("%H:%M:%S")


def main():
    import argparse
    from db import get_db
    ap = argparse.ArgumentParser()
    ap.add_argument("--share", action="store_true", help="let other laptops on the same network open the dashboard")
    args = ap.parse_args()
    dl = DataLayer(get_db())
    dl.ensure_indexes()
    app = build_app(dl)
    host = "0.0.0.0" if args.share else config.DASH_HOST
    print(f"Dashboard on this laptop: http://127.0.0.1:{config.DASH_PORT}  (refreshes every {config.DASH_REFRESH_MS / 1000:g} s)")
    if args.share:
        print(f"  SHARING is on: on the other laptop open http://{config.lan_ip()}:{config.DASH_PORT}")
    app.run(host=host, port=config.DASH_PORT, debug=False)


if __name__ == "__main__":
    main()
