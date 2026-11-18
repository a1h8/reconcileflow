"""In the manner of CrossTrace (arXiv 2508.11342, section III-B, Algorithm 1):
a ReconcileFlow rewrite from the paper's text, its code not being public,
with the seven choices declared in phase 4 spec 8.27.1. Never to be quoted
as CrossTrace's result.

Runs where numpy, scipy and scikit-learn are (TraceWeaver's environment), from
``probe_compare.export_lives``; judged by ``probe_compare.from_file``:

  python crosstrace_like.py <lives.json> <links.json>
  python crosstrace_like.py --self-test

One proxy, one call per request:
1. candidates: calls nested in the request; d1 = call start - request start,
   d4 = request end - call end;
2. mu_k: mean of d_k over the requests' earliest candidate; candidates with
   d1 >= delta * mu_1 dropped (delta = 4);
3-5. central deviation C = sum |d_k - mu_k| / mu_k; a request is of high
   certainty with one candidate, or when its two lowest C differ by 20 % or
   more; one law per delay fitted on their best candidates, the lowest BIC of
   normal, log-normal, exponential and Gaussian mixtures of 1 to 20;
6. score P = sum log f_k(d_k); requests by decreasing gap between their two
   best P; each takes its best call not yet taken (elimination in place of
   the paper's exhaustive conflict search);
7. a request left with no call is not linked.
"""

import json
import math
import sys
from bisect import bisect_left, bisect_right
from pathlib import Path

DELTA = 4
GAP = 0.2


def _fit(values):
    """The law of one delay: (name, logpdf), lowest BIC."""
    import numpy as np
    from scipy import stats
    from sklearn.mixture import GaussianMixture

    x = np.asarray(values, dtype=float)
    n = len(x)
    fits = []
    mu, sd = x.mean(), max(x.std(), 1e-9)
    fits.append(("normal", 2, lambda v: stats.norm.logpdf(v, mu, sd)))
    if (x > 0).all():
        s, loc, scale = stats.lognorm.fit(x, floc=0)
        fits.append(("lognormal", 2, lambda v: stats.lognorm.logpdf(v, s, loc, scale)))
    if (x >= 0).all():
        scale = max(x.mean(), 1e-9)
        fits.append(("exponential", 1, lambda v: stats.expon.logpdf(v, 0, scale)))
    col = x.reshape(-1, 1)
    for k in range(1, min(20, len(np.unique(x))) + 1):
        g = GaussianMixture(n_components=k, random_state=100).fit(col)
        fits.append(
            (
                f"gmm{k}",
                3 * k - 1,
                lambda v, g=g: g.score_samples(np.asarray(v, float).reshape(-1, 1)),
            )
        )
    best = None
    for name, params, logpdf in fits:
        ll = float(np.sum(logpdf(x)))
        bic = params * math.log(n) - 2 * ll
        if best is None or bic < best[0]:
            best = (bic, name, logpdf)
    return best[1], best[2]


def link(lives):
    import numpy as np

    reqs, calls = lives["requests"], lives["calls"]
    order = sorted(range(len(calls)), key=lambda j: (calls[j][0], j))
    starts = [calls[j][0] for j in order]
    cands = {}
    for i, (s, e) in enumerate(reqs):
        lo, hi = bisect_left(starts, s), bisect_right(starts, e)
        c = [order[k] for k in range(lo, hi) if calls[order[k]][1] <= e]
        if c:
            cands[i] = c

    def d(i, j):
        return calls[j][0] - reqs[i][0], reqs[i][1] - calls[j][1]

    early = [d(i, min(c, key=lambda j: calls[j][0])) for i, c in cands.items()]
    mu = [max(sum(x[k] for x in early) / len(early), 1e-9) for k in (0, 1)]
    for i in list(cands):
        cands[i] = [j for j in cands[i] if d(i, j)[0] < DELTA * mu[0]]
        if not cands[i]:
            del cands[i]

    def central(i, j):
        a, b = d(i, j)
        return abs(a - mu[0]) / mu[0] + abs(b - mu[1]) / mu[1]

    train = ([], [])
    for i, c in cands.items():
        cs = sorted((central(i, j), j) for j in c)
        high = len(cs) == 1 or (cs[0][0] > 0 and (cs[1][0] - cs[0][0]) / cs[0][0] >= GAP)
        if high:
            a, b = d(i, cs[0][1])
            train[0].append(a)
            train[1].append(b)
    laws = [_fit(train[0]), _fit(train[1])]

    scores = {}
    for i, c in cands.items():
        ds = np.array([d(i, j) for j in c])
        p = laws[0][1](ds[:, 0]) + laws[1][1](ds[:, 1])
        scores[i] = sorted(zip(p.tolist(), c, strict=True), key=lambda t: (-t[0], t[1]))

    def gap(i):
        s = scores[i]
        return math.inf if len(s) == 1 else s[0][0] - s[1][0]

    taken, links = set(), {}
    for i in sorted(scores, key=lambda i: (-gap(i), i)):
        for _, j in scores[i]:
            if j not in taken:
                taken.add(j)
                links[i] = j
                break
    return {
        "links": links,
        "skips": [],
        "laws": [laws[0][0], laws[1][0]],
        "high_certainty": len(train[0]),
    }


def self_test():
    import random

    random.seed(1)
    reqs, calls = [], []
    for k in range(400):  # sequential: one candidate each
        s = k * 100.0
        dur = random.uniform(20, 60)
        reqs.append([s, s + dur + 10])
        calls.append([s + 3 + random.uniform(0, 1), s + 3 + dur])
    got = link({"requests": reqs, "calls": calls})
    assert got["links"] == {i: i for i in range(400)}, "sequential requests"
    # Two requests, both calls inside both: it still links (it guesses).
    two = link({"requests": [[0, 100], [5, 105]] * 1, "calls": [[10, 50], [20, 60]]})
    assert len(two["links"]) == 2
    print("self-test ok")


if __name__ == "__main__":
    if sys.argv[1] == "--self-test":
        self_test()
    else:
        out = link(json.loads(Path(sys.argv[1]).read_text()))
        Path(sys.argv[2]).write_text(json.dumps(out))
