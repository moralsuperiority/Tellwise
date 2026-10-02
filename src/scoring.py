"""Risk scoring. Every point added to a score comes with a human-readable reason,
so each alert is explainable. Per-user behaviour lives in Redis."""
import math

MIN_HISTORY = 5          # transactions needed before amount/travel checks k
REVIEW_AT = 40
BLOCK_AT = 80


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def score_transaction(r, tx):
    """r: redis client (decode_responses=True). tx: dict with ts_epoch added.
    Returns (risk_score, decision, reasons)."""
    uid, ts, amount = tx["user_id"], tx["ts_epoch"], tx["amount"]
    profile_key = f"user:{uid}:profile"
    merchants_key = f"user:{uid}:merchants"

    p = r.hgetall(profile_key)
    n = int(p.get("n", 0))
    mean = float(p.get("mean", 0.0))
    m2 = float(p.get("m2", 0.0))
    reasons = []

    def add(code, points, detail):
        reasons.append({"code": code, "points": points, "detail": detail})

    # 1) Amount far above this user's own normal (running mean/std via Welford)
    if n >= MIN_HISTORY:
        std = max(math.sqrt(m2 / (n - 1)), 0.1 * mean, 1.0)
        z = (amount - mean) / std
        if z > 6:
            add("AMOUNT_SPIKE", 60, f"amount is {z:.1f} std-devs above this user's normal (avg {mean:.0f})")
        elif z > 3:
            add("AMOUNT_SPIKE", 40, f"amount is {z:.1f} std-devs above this user's normal (avg {mean:.0f})")

    # 2) Velocity: transactions by this user in the last 10 seconds
    rkey = f"user:{uid}:recent"
    pipe = r.pipeline()
    pipe.zremrangebyscore(rkey, 0, ts - 10)
    pipe.zadd(rkey, {tx["txn_id"]: ts})
    pipe.expire(rkey, 30)
    pipe.zcard(rkey)
    count = pipe.execute()[-1]
    if count >= 5:
        add("VELOCITY_BURST", 40, f"{count} transactions in the last 10 seconds")

    # 3) Impossible travel vs the last trusted transaction
    #    (skipped until the user has some history, so a poisoned first transaction can heal)
    if "last_ts" in p and n >= 2:
        km = haversine_km(float(p["last_lat"]), float(p["last_lon"]), tx["lat"], tx["lon"])
        hours = max((ts - float(p["last_ts"])) / 3600, 1 / 3600)
        speed = km / hours
        if km > 200 and speed > 900:
            add("IMPOSSIBLE_TRAVEL", 50, f"{km:.0f} km from last trusted location in {hours * 60:.1f} min ({speed:.0f} km/h)")

    # 4) Brand-new merchant combined with a bigger-than-usual amount
    if n >= MIN_HISTORY and not r.sismember(merchants_key, tx["merchant"]) and amount > 3 * mean:
        add("NEW_MERCHANT_HIGH_AMOUNT", 10, "first time at this merchant and amount is over 3x the user's average")

    # 5) Fraud-ring signal: one device touching many different accounts within an hour
    dkey = f"device:{tx['device_id']}:users"
    pipe = r.pipeline()
    pipe.sadd(dkey, uid)
    pipe.expire(dkey, 3600)
    pipe.scard(dkey)
    users_on_device = pipe.execute()[-1]
    if users_on_device >= 3:
        add("SHARED_DEVICE_RING", 40, f"device used by {users_on_device} different accounts in the last hour")

    score = min(100, sum(x["points"] for x in reasons))
    decision = "BLOCK" if score >= BLOCK_AT else "REVIEW" if score >= REVIEW_AT else "ALLOW"

    # Poisoning protection v2 (clipped learning):
    # - amount stats learn from every non-BLOCK transaction, but a suspicious amount
    #   is clipped to 3 std-devs above normal: it can nudge the baseline, never hijack it
    # - location / merchant / last_ts only learn from fully trusted (ALLOW) transactions
    if decision != "BLOCK":
        learn_amount = amount
        if n >= MIN_HISTORY:
            std = max(math.sqrt(m2 / (n - 1)), 0.1 * mean, 1.0)
            learn_amount = min(amount, mean + 3 * std)
        n += 1
        delta = learn_amount - mean
        mean += delta / n
        m2 += delta * (learn_amount - mean)
        fields = {"n": n, "mean": mean, "m2": m2}
        if decision == "ALLOW":
            fields.update({"last_ts": ts, "last_lat": tx["lat"],
                           "last_lon": tx["lon"], "last_city": tx["city"]})
            r.sadd(merchants_key, tx["merchant"])
        r.hset(profile_key, mapping=fields)

    return score, decision, reasons
