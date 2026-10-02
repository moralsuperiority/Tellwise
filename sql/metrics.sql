-- Precision / recall of the engine against simulator ground truth
SELECT
    COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND true_label)      AS true_positives,
    COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND NOT true_label)  AS false_positives,
    COUNT(*) FILTER (WHERE decision = 'ALLOW'  AND true_label)      AS missed_fraud,
    ROUND(100.0 * COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND true_label)
          / NULLIF(COUNT(*) FILTER (WHERE decision <> 'ALLOW'), 0), 1) AS precision_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND true_label)
          / NULLIF(COUNT(*) FILTER (WHERE true_label), 0), 1)          AS recall_pct
FROM scored_transactions;

-- End-to-end latency
SELECT
    ROUND(AVG(latency_ms)) AS avg_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_ms
FROM scored_transactions;

-- Which reason codes fire most?
SELECT r->>'code' AS reason, COUNT(*)
FROM scored_transactions, jsonb_array_elements(reasons) r
GROUP BY 1 ORDER BY 2 DESC;
