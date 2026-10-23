-- Example analysis over state/hospitals.db (run all with: python run_queries.py).
-- The loader turns placeholders ('Not Available', 'N/A', ...) into NULL and types
-- numeric columns INTEGER/REAL, so plain AVG()/COUNT(col) just work. Identifiers
-- (facility_id, zip_code, ...) stay TEXT to keep leading zeros. The views below only
-- rename and select columns; no CAST is needed.

-- name: drop_view_hospitals
DROP VIEW IF EXISTS v_hospitals;

-- name: view_hospitals
CREATE VIEW v_hospitals AS
SELECT facility_id, facility_name, city_town, state, hospital_type, hospital_ownership,
       hospital_overall_rating AS stars
FROM hospital_general_information_xubh_q36u;

-- name: drop_view_readmissions
DROP VIEW IF EXISTS v_readmissions;

-- name: view_readmissions
CREATE VIEW v_readmissions AS
SELECT facility_id, measure_id, measure_name, compared_to_national, score, denominator
FROM unplanned_hospital_visits_hospital_632h_zaca;

-- name: avg_stars_by_state (states with at least 20 rated hospitals)
SELECT state, ROUND(AVG(stars), 2) AS avg_stars, COUNT(*) AS rated_hospitals
FROM v_hospitals
WHERE stars IS NOT NULL
GROUP BY state HAVING rated_hospitals >= 20
ORDER BY avg_stars DESC;

-- Note: 'Hybrid_HWR' (hospital-wide readmission) is 'Not Available' for every hospital in
-- the current release, so these queries use READM_30_PN (pneumonia), the best-populated
-- readmission measure. See data_coverage at the bottom to pick another.

-- name: worst_pneumonia_readmission (10 highest rates, joined to hospital name/state)
SELECT h.facility_name, h.state, r.score AS readmission_rate_pct, r.denominator AS patients
FROM v_readmissions r
JOIN v_hospitals h USING (facility_id)
WHERE r.measure_id = 'READM_30_PN' AND r.score IS NOT NULL AND r.denominator >= 250
ORDER BY r.score DESC
LIMIT 10;

-- name: readmission_by_ownership (does ownership type matter?)
SELECT h.hospital_ownership, ROUND(AVG(r.score), 2) AS avg_readmission_pct,
       COUNT(*) AS hospitals, ROUND(AVG(h.stars), 2) AS avg_stars
FROM v_readmissions r
JOIN v_hospitals h USING (facility_id)
WHERE r.measure_id = 'READM_30_PN' AND r.score IS NOT NULL
GROUP BY h.hospital_ownership
HAVING hospitals >= 30
ORDER BY avg_readmission_pct;

-- name: worse_than_national_by_state (share of readmission measures rated worse than national)
SELECT h.state,
       COUNT(*) AS measures,
       SUM(r.compared_to_national LIKE 'Worse%') AS worse,
       ROUND(100.0 * SUM(r.compared_to_national LIKE 'Worse%') / COUNT(*), 1) AS pct_worse
FROM v_readmissions r
JOIN v_hospitals h USING (facility_id)
WHERE r.compared_to_national IS NOT NULL
GROUP BY h.state HAVING measures >= 200
ORDER BY pct_worse DESC
LIMIT 10;

-- name: data_coverage (how much of each measure is actually populated)
SELECT measure_id, COUNT(*) AS rows_total, SUM(score IS NOT NULL) AS rows_with_score,
       ROUND(100.0 * SUM(score IS NOT NULL) / COUNT(*), 1) AS pct_populated
FROM v_readmissions
GROUP BY measure_id
ORDER BY rows_total DESC;
