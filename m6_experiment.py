"""
m6_experiment.py - Milestone 6: the live-vs-stale experiment (offline, accelerated, no Kafka).

THREE SETUPS, SAME CUSTOMERS:
  none   customers see no recommendations
  stale  recommendations computed from a snapshot refreshed hourly   (the control)
  live   recommendations computed from the live event stream         (the treatment)
Every customer follows the same organic session script in every setup, so differences come only from
the recommender (a paired comparison).  Scenarios change ONE assumption: how strongly relevance raises
the chance a customer clicks (LIFT): no_relevance=0, low=1, base=2, high=3.   "no_relevance" shows what staleness costs even if relevance did not matter at all
(a stale list sometimes suggests an item already in the cart).  A separate A/A check (live vs live) must show ~0.

    python m6_experiment.py                 # about 5 to 10 minutes
    python m6_experiment.py --quick         # about 1 minute, rough numbers
"""
import argparse
import heapq
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

import config
from common import make_event
from interactive import ResponseModel, session_gen
from recommender import Recommender, load_base_from_csv, pair_counts_from_simulation
from simulator import Simulator

T0 = 1_800_000_000.0                       # simulated clock starts here (any epoch works)
SCENARIOS = {"no_relevance": 0.0, "low": 1.0, "base": 2.0, "high": 3.0}


class OfflineCtx:
    def __init__(self, world, model, rec, mode, seed, sink):
        self.world, self.model, self.rec, self.mode, self.seed = world, model, rec, mode, seed
        self.recs_enabled = mode in ("stale", "live")
        self._pending, self.sink = {}, sink

    def emit(self, ts, et, user, sid, payload):
        ev = make_event(et, user, sid, ts, payload)
        if self.rec:
            self.rec.process(ev)
        return ev

    def request_rec(self, ev):
        self._pending[ev["event_id"]] = self.rec.recommend(ev, view=self.mode)

    def get_rec(self, ev):
        return self._pending.pop(ev["event_id"], None)

    def finish(self, summary):
        self.sink.append(summary)


def build_scripts(sim, n, sessions_per_hour, seed):
    rng = random.Random(seed)
    t, arrivals = T0, []
    for _ in range(n):
        t += rng.expovariate(sessions_per_hour / 3600.0)
        arrivals.append(t)
    scripts = [sim.build_session_seeded(arrivals[k], seed * 1000003 + k) for k in range(n)]
    return arrivals, scripts


def run_arm(sim, base, pairs, arrivals, scripts, mode, lift, seed):
    products, user_cats, ratings = base
    clock = {"t": T0}
    rec = None
    if mode != "none":
        rec = Recommender(products, user_cats, ratings, pairs, trend_window=config.TRENDING_WINDOW_SEC,
                          trend_slide=config.TRENDING_SLIDE_SEC, stale_refresh=config.STALE_REFRESH_SEC,
                          now_fn=lambda: clock["t"])
    out = []
    ctx = OfflineCtx(sim.world, ResponseModel(sim.world, lift), rec, mode, seed, out)
    heap, seq, k = [], 0, 0
    n = len(scripts)
    while heap or k < n:
        nxt = arrivals[k] if k < n else float("inf")
        if heap and heap[0][0] <= nxt:
            wake, _, gen = heapq.heappop(heap)
        else:
            gen, wake = session_gen(scripts[k], k, ctx), arrivals[k]
            k += 1
        clock["t"] = max(clock["t"], wake)
        try:
            seq += 1
            heapq.heappush(heap, (next(gen), seq, gen))
        except StopIteration:
            pass
    out.sort(key=lambda s: s["sidx"])
    return out


def arrays(summaries, first):
    s = summaries[first:]
    return {k: np.array([x[k] for x in s], dtype=float) for k in ("ordered", "revenue", "units", "shown", "clicks", "adds")}


def metrics(a, idx=None):
    g = (lambda v: v) if idx is None else (lambda v: v[idx])
    o, rev, un = g(a["ordered"]), g(a["revenue"]), g(a["units"])
    orders = o.sum()
    return {"conversion": o.mean(), "aov": rev.sum() / orders if orders else 0.0, "rps": rev.mean(),
            "units_per_order": un.sum() / orders if orders else 0.0}


def rel_uplift(m_new, m_old, key):
    return m_new[key] / m_old[key] - 1 if m_old[key] else float("nan")


def point(a_new, a_old):
    mn, mo = metrics(a_new), metrics(a_old)
    return {k: rel_uplift(mn, mo, k) for k in ("conversion", "aov", "rps")}


T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}


def interval(vals):
    """mean and 95% interval ACROSS independent replications (customers are not independent of each other,
    because recommended items feed back into 'trending', so a single run's own interval would be too narrow)."""
    v = np.array(vals, dtype=float)
    m = float(np.nanmean(v))
    if len(v) < 2:
        return {"rel": m, "lo": m, "hi": m, "reps": v.tolist()}
    h = T975.get(len(v) - 1, 2.0) * float(np.nanstd(v, ddof=1)) / np.sqrt(len(v))
    return {"rel": m, "lo": m - h, "hi": m + h, "reps": v.tolist()}


def rec_stats(a):
    sh, cl, ad = a["shown"].sum(), a["clicks"].sum(), a["adds"].sum()
    return {"shown_per_session": sh / len(a["shown"]), "ctr": cl / sh if sh else 0.0, "add_rate": ad / sh if sh else 0.0}


def mean_stats(rows):
    return {k: float(np.mean([r[k] for r in rows])) for k in rows[0]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replications", type=int, default=5, help="independent repeats of the whole experiment")
    ap.add_argument("--sessions-per-hour", type=float, default=1000)
    ap.add_argument("--warmup-hours", type=float, default=2.0, help="not measured; lets the stale snapshot fill with real activity")
    ap.add_argument("--hours", type=float, default=3.0, help="measured period per replication")
    ap.add_argument("--seed", type=int, default=config.RANDOM_SEED)
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--no-db", action="store_true", help="do not save the result to MongoDB")
    a = ap.parse_args()
    if a.quick:
        a.replications, a.sessions_per_hour, a.warmup_hours, a.hours = 3, 500, 1.2, 1.5

    n = int((a.warmup_hours + a.hours) * a.sessions_per_hour)
    print(f"{a.replications} independent replications x {n} customer sessions each "
          f"({a.warmup_hours:g} h warm-up not measured, {a.hours:g} h measured), {a.sessions_per_hour:g} sessions per simulated hour")
    acc = {name: {"live_vs_stale": [], "stale_vs_none": [], "stale": [], "live": []} for name in SCENARIOS}
    aa, none_stats = [], []
    t0 = time.time()
    for rep in range(a.replications):
        seed = a.seed + rep
        sim = Simulator(seed=seed)
        base = load_base_from_csv(sim)
        pairs = pair_counts_from_simulation(Simulator(seed=seed))
        arrivals, scripts = build_scripts(sim, n, a.sessions_per_hour, seed)
        first = sum(1 for t in arrivals if t < T0 + a.warmup_hours * 3600)
        none = arrays(run_arm(sim, base, pairs, arrivals, scripts, "none", 0.0, seed), first)
        none_stats.append(metrics(none))
        for name, lift in SCENARIOS.items():
            stale = arrays(run_arm(sim, base, pairs, arrivals, scripts, "stale", lift, seed), first)
            live = arrays(run_arm(sim, base, pairs, arrivals, scripts, "live", lift, seed), first)
            acc[name]["live_vs_stale"].append(point(live, stale))
            acc[name]["stale_vs_none"].append(point(stale, none))
            acc[name]["stale"].append({**metrics(stale), **rec_stats(stale)})
            acc[name]["live"].append({**metrics(live), **rec_stats(live)})
            if name == "base":      # A/A check: the SAME live recommender twice; only the customers' random responses differ
                live2 = arrays(run_arm(sim, base, pairs, arrivals, scripts, "live", lift, seed + 7919), first)
                aa.append(point(live, live2))
        print(f"  replication {rep + 1}/{a.replications} done ({time.time() - t0:.0f}s)")

    keys = ("conversion", "aov", "rps")
    result = {"created_at": datetime.now(timezone.utc), "params": {
        "replications": a.replications, "sessions_per_replication": n, "measured_sessions_per_replication": n - first,
        "sessions_per_hour": a.sessions_per_hour, "warmup_hours": a.warmup_hours, "measured_hours": a.hours, "seed": a.seed,
        "stale_refresh_sec": config.STALE_REFRESH_SEC, "base_ctr": config.BASE_CTR, "base_add": config.BASE_CLICK_TO_CART,
        "scenario_lift": SCENARIOS}, "baseline_none": mean_stats(none_stats), "scenarios": {},
        "aa_check": {k: interval([r[k] for r in aa]) for k in keys}}
    for name, lift in SCENARIOS.items():
        result["scenarios"][name] = {"lift": lift, "stale": mean_stats(acc[name]["stale"]), "live": mean_stats(acc[name]["live"]),
            "live_vs_stale": {k: interval([r[k] for r in acc[name]["live_vs_stale"]]) for k in keys},
            "stale_vs_none": {k: interval([r[k] for r in acc[name]["stale_vs_none"]]) for k in keys}}

    # ---------------------------------------------------------------- report
    p = lambda x: f"{100 * x:+.1f}%"
    print(f"\nRESULTS: live vs stale, relative change (average of {a.replications} independent replications, 95% interval across them)")
    print(f"{'scenario':<13}{'lift':>5}  {'conversion':>24}  {'avg order value':>24}  {'revenue / session':>24}")
    for name, sc in result["scenarios"].items():
        u = sc["live_vs_stale"]
        cell = lambda k: f"{p(u[k]['rel'])} [{p(u[k]['lo'])}, {p(u[k]['hi'])}]"
        print(f"{name:<13}{sc['lift']:>5g}  {cell('conversion'):>24}  {cell('aov'):>24}  {cell('rps'):>24}")
    print("\nrecommendation click-through / add rate (live vs stale):")
    for name, sc in result["scenarios"].items():
        print(f"  {name:<13} live {100 * sc['live']['ctr']:.1f}% / {100 * sc['live']['add_rate']:.1f}%   stale {100 * sc['stale']['ctr']:.1f}% / {100 * sc['stale']['add_rate']:.1f}%")
    ch = result["aa_check"]
    ok = all(ch[k]["lo"] <= 0 <= ch[k]["hi"] for k in ch)
    print("\nSANITY, A/A check (the same live recommender twice; only random customer responses differ):")
    print("  " + "   ".join(f"{k} {p(ch[k]['rel'])} [{p(ch[k]['lo'])}, {p(ch[k]['hi'])}]" for k in ch))
    print("  " + ("all intervals contain 0: the experiment is not inventing an effect." if ok else
                  "an interval excludes 0: this happens by chance about 1 time in 20; re-run with another --seed before concluding anything."))
    print("\nHOW TO READ: 'no_relevance' is the live advantage that does NOT depend on relevance; the gap to low/base/high is what the")
    print("relevance assumption adds. The size of the effect is driven by that assumption, so present it as a range, not as a finding.")

    Path("results").mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    Path(f"results/experiment_{stamp}.json").write_text(json.dumps(json.loads(json.dumps(result, default=str)), indent=2))
    print(f"\nsaved results/experiment_{stamp}.json")
    if not a.no_db:
        try:
            from db import get_db
            get_db()["experiment_results"].insert_one(result)
            print("saved to MongoDB (collection experiment_results): the dashboard will show it")
        except SystemExit as e:
            print("could not save to MongoDB:", e)


if __name__ == "__main__":
    main()
