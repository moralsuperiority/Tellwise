
import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="Real-Time Fraud Detection Engine", page_icon="🛡️", layout="wide")

DB = dict(host="localhost", port=5432, dbname="fraud", user="fraud", password="fraud")


def q(sql):
    """Run a query and return a DataFrame."""
    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


def render_live():
    try:
        k = q("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE decision <> 'ALLOW') AS flagged,
                   COALESCE(AVG(latency_ms), 0) AS avg_ms,
                   COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms), 0) AS p95_ms,
                   COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND true_label) AS tp,
                   COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND NOT true_label) AS fp,
                   COUNT(*) FILTER (WHERE decision = 'ALLOW' AND true_label) AS fn
            FROM scored_transactions
        """).iloc[0]
        per_min = q("""
            SELECT date_trunc('minute', event_ts) AS minute,
                   COUNT(*) FILTER (WHERE decision <> 'ALLOW') AS flagged
            FROM scored_transactions
            WHERE event_ts > now() - interval '30 minutes'
            GROUP BY 1 ORDER BY 1
        """)
        reasons = q("""
            SELECT r->>'code' AS reason, COUNT(*) AS hits
            FROM scored_transactions, jsonb_array_elements(reasons) r
            GROUP BY 1 ORDER BY 2 DESC
        """)
        alerts = q("""
            SELECT event_ts, user_id, amount, city, risk_score, decision, reasons
            FROM alerts ORDER BY event_ts DESC LIMIT 25
        """)
    except psycopg2.Error:
        st.error("Can't reach PostgreSQL. Is it running? Try `docker compose up -d` in the project folder.")
        return

    total, flagged = int(k["total"]), int(k["flagged"])
    tp, fp, fn = int(k["tp"]), int(k["fp"]), int(k["fn"])
    precision = f"{100 * tp / (tp + fp):.1f}%" if tp + fp else "n/a"
    recall = f"{100 * tp / (tp + fn):.1f}%" if tp + fn else "n/a"
    flag_rate = f"{100 * flagged / total:.1f}% of traffic" if total else None

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Transactions scored", f"{total:,}")
    c2.metric("Flagged", f"{flagged:,}", flag_rate, delta_color="off")
    c3.metric("Precision", precision)
    c4.metric("Recall", recall)
    c5.metric("Avg latency", f"{float(k['avg_ms']):.0f} ms")
    c6.metric("p95 latency", f"{float(k['p95_ms']):.0f} ms")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Flagged per minute (last 30 min)")
        if per_min.empty:
            st.info("No data yet. Start the producer and the stream job.")
        else:
            st.line_chart(per_min.set_index("minute")["flagged"])
    with right:
        st.subheader("Why transactions get flagged")
        if reasons.empty:
            st.info("No flagged transactions yet.")
        else:
            reasons["hits"] = reasons["hits"].astype(int)
            st.bar_chart(reasons.set_index("reason")["hits"])

    st.subheader("Live alert feed (newest first)")
    if alerts.empty:
        st.info("No alerts yet.")
    else:
        alerts["amount"] = alerts["amount"].astype(float)
        alerts["time (UTC)"] = pd.to_datetime(alerts["event_ts"]).dt.strftime("%H:%M:%S")
        alerts["why"] = alerts["reasons"].apply(lambda rs: " | ".join(x["detail"] for x in rs))
        alerts["decision"] = alerts["decision"].map({"BLOCK": "🟥 BLOCK", "REVIEW": "🟧 REVIEW"})
        st.dataframe(
            alerts[["time (UTC)", "user_id", "amount", "city", "risk_score", "decision", "why"]],
            hide_index=True,
        )


st.title("🛡️ Real-Time Fraud Detection Engine")
st.caption("Simulated card transactions streamed through Kafka and Spark, scored with Redis, stored in PostgreSQL.")

auto = st.sidebar.checkbox("Auto-refresh every 3 s", value=True)
st.sidebar.caption("Untick this if you want to scroll or select text without the page refreshing.")

tab_live, tab_about = st.tabs(["Live dashboard", "How it works"])

with tab_live:
    live = st.fragment(run_every=3 if auto else None)(render_live)
    live()

with tab_about:
    st.markdown("""
### Architecture
```
Producer (fake transactions) -> Kafka -> Spark Structured Streaming -> PostgreSQL -> this dashboard
                                              |
                                              +-> Redis (per-user behaviour memory)
```

### What the engine checks
| Rule | What it looks for |
|---|---|
| AMOUNT_SPIKE | Amount far above this user's own running average |
| VELOCITY_BURST | Many transactions by one user within 10 seconds |
| IMPOSSIBLE_TRAVEL | Far-away location faster than a plane could travel |
| SHARED_DEVICE_RING | One device used by several different accounts within an hour |
| NEW_MERCHANT_HIGH_AMOUNT | First time at a merchant, with a bigger-than-usual amount |

Every flag stores a plain-English reason, so each alert is explainable.

### Design choices
- **Clipped learning:** a suspicious amount can only nudge a user's baseline, never hijack it.
- **Trusted-only memory:** location and merchant history only learn from fully trusted transactions.
- **Measured honestly:** the simulator keeps ground-truth labels, so precision and recall are measured and not guessed.

### Known limitations
- All data is simulated.
- Cold start: the engine can't judge users it has barely seen.
- Burst and ring fraud is only caught once enough activity builds up, which limits recall.
""")
