DROP TABLE IF EXISTS #TTAR;

DROP TABLE IF EXISTS #TTAR_to_erafy;

DROP TABLE IF EXISTS #TTAR_era_overlaps;

DROP TABLE IF EXISTS #TTAR_erafied;

DROP TABLE IF EXISTS #subgroup_person;

DROP TABLE IF EXISTS #excluded_tar_cohort;

DROP TABLE IF EXISTS #exc_TTAR_o;

DROP TABLE IF EXISTS #ex_TTAR_o_overlaps;

DROP TABLE IF EXISTS #exc_TTAR_o_to_erafy;

DROP TABLE IF EXISTS #exc_TTAR_o_erafied;

DROP TABLE IF EXISTS #outcome_smry;

DROP TABLE IF EXISTS #excluded_person_days;

DROP TABLE IF EXISTS #incidence_raw;

DROP TABLE IF EXISTS #tscotar_ref;

DROP TABLE IF EXISTS #incidence_subgroups;

DROP TABLE IF EXISTS #incidence_summary;

-- 1) create T + TAR periods
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT cohort_definition_id,
	tar_id,
	subject_id,
	start_date,
	end_date
INTO #TTAR
FROM (
	SELECT tc1.cohort_definition_id,
		tar1.tar_id,
		subject_id,
		CASE 
			WHEN tar1.start_anchor = 'cohort start'
				THEN CASE 
						WHEN DATEADD(dd, tar1.risk_window_start, tc1.cohort_start_date) < op1.observation_period_end_date
							THEN DATEADD(dd, tar1.risk_window_start, tc1.cohort_start_date)
						WHEN DATEADD(dd, tar1.risk_window_start, tc1.cohort_start_date) >= op1.observation_period_end_date
							THEN op1.observation_period_end_date
						END
			WHEN tar1.start_anchor = 'cohort end'
				THEN CASE 
						WHEN DATEADD(dd, tar1.risk_window_start, tc1.cohort_end_date) < op1.observation_period_end_date
							THEN DATEADD(dd, tar1.risk_window_start, tc1.cohort_end_date)
						WHEN DATEADD(dd, tar1.risk_window_start, tc1.cohort_end_date) >= op1.observation_period_end_date
							THEN op1.observation_period_end_date
						END
			ELSE NULL --shouldn't get here if tar set properly
			END AS start_date,
		CASE 
			WHEN tar1.end_anchor = 'cohort start'
				THEN CASE 
						WHEN DATEADD(dd, tar1.risk_window_end, tc1.cohort_start_date) < op1.observation_period_end_date
							THEN DATEADD(dd, tar1.risk_window_end, tc1.cohort_start_date)
						WHEN DATEADD(dd, tar1.risk_window_end, tc1.cohort_start_date) >= op1.observation_period_end_date
							THEN op1.observation_period_end_date
						END
			WHEN tar1.end_anchor = 'cohort end'
				THEN CASE 
						WHEN DATEADD(dd, tar1.risk_window_end, tc1.cohort_end_date) < op1.observation_period_end_date
							THEN DATEADD(dd, tar1.risk_window_end, tc1.cohort_end_date)
						WHEN DATEADD(dd, tar1.risk_window_end, tc1.cohort_end_date) >= op1.observation_period_end_date
							THEN op1.observation_period_end_date
						END
			ELSE NULL --shouldn't get here if tar set properly
			END AS end_date
	FROM (
		SELECT tar_id,
			start_anchor,
			risk_window_start,
			end_anchor,
			risk_window_end
		FROM #tar_ref
		) tar1,
		(
			SELECT cohort_definition_id,
				subject_id,
				cohort_start_date,
				cohort_end_date
			FROM @target_cohort_database_schema.@target_cohort_table
			WHERE cohort_definition_id IN (SELECT target_cohort_definition_id FROM #target_ref)
			) tc1
	INNER JOIN @cdm_database_schema.observation_period op1 ON tc1.subject_id = op1.person_id
		AND tc1.cohort_start_date >= op1.observation_period_start_date
		AND tc1.cohort_start_date <= op1.observation_period_end_date
	) TAR
WHERE TAR.start_date <= TAR.end_date;

/*
2) create table to store era-fied at-risk periods
  UNION all periods that don't require erafying 
  with era-fy records that require it
*/
--find the records that need to be era-fied
--era-building script for the 'TTAR_to_erafy' records
--insert records from era-building script into #TTAR_erafied
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT t1.cohort_definition_id,
	t1.tar_id,
	t1.subject_id,
	t1.start_date,
	t1.end_date
INTO #TTAR_to_erafy
FROM #TTAR t1
INNER JOIN #TTAR t2 ON t1.cohort_definition_id = t2.cohort_definition_id
	AND t1.tar_id = t2.tar_id
	AND t1.subject_id = t2.subject_id
	AND t1.start_date <= t2.end_date
	AND t1.end_date >= t2.start_date
	AND t1.start_date <> t2.start_date;

--HINT DISTRIBUTE_ON_KEY(subject_id)
WITH cteEndDates (
	cohort_definition_id,
	tar_id,
	subject_id,
	end_date
	)
AS (
	SELECT cohort_definition_id,
		tar_id,
		subject_id,
		event_date AS end_date
	FROM (
		SELECT cohort_definition_id,
			tar_id,
			subject_id,
			event_date,
			SUM(event_type) OVER (
				PARTITION BY cohort_definition_id,
				tar_id,
				subject_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
				) AS interval_status
		FROM (
			SELECT cohort_definition_id,
				tar_id,
				subject_id,
				start_date AS event_date,
				- 1 AS event_type
			FROM #TTAR_to_erafy
			
			UNION ALL
			
			SELECT cohort_definition_id,
				tar_id,
				subject_id,
				end_date AS event_date,
				1 AS event_type
			FROM #TTAR_to_erafy
			) RAWDATA
		) e
	WHERE interval_status = 0
	),
cteEnds (
	cohort_definition_id,
	tar_id,
	subject_id,
	start_date,
	end_date
	)
AS (
	SELECT c.cohort_definition_id,
		c.tar_id,
		c.subject_id,
		c.start_date,
		MIN(e.end_date) AS end_date
	FROM #TTAR_to_erafy c
	INNER JOIN cteEndDates e ON c.subject_id = e.subject_id
		AND c.cohort_definition_id = e.cohort_definition_id
		AND c.tar_id = e.tar_id
		AND e.end_date >= c.start_date
	GROUP BY c.cohort_definition_id,
		c.tar_id,
		c.subject_id,
		c.start_date
	)
SELECT cohort_definition_id,
	tar_id,
	subject_id,
	min(start_date) AS start_date,
	end_date
INTO #TTAR_era_overlaps
FROM cteEnds
GROUP BY cohort_definition_id,
	tar_id,
	subject_id,
	end_date;

--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT cohort_definition_id,
	tar_id,
	subject_id,
	start_date,
	@tarEndDateExpression
INTO #TTAR_erafied
FROM (
	SELECT cohort_definition_id,
		tar_id,
		subject_id,
		start_date,
		end_date
	FROM #TTAR_era_overlaps
	
	UNION ALL
	
	--records that were already erafied and just need to be brought over directly
	SELECT DISTINCT t1.cohort_definition_id,
		t1.tar_id,
		t1.subject_id,
		t1.start_date,
		t1.end_date
	FROM #TTAR t1
	LEFT JOIN #TTAR t2 ON t1.cohort_definition_id = t2.cohort_definition_id
		AND t1.tar_id = t2.tar_id
		AND t1.subject_id = t2.subject_id
		AND t1.start_date <= t2.end_date
		AND t1.end_date >= t2.start_date
		AND t1.start_date <> t2.start_date
	WHERE t2.subject_id IS NULL
	) T 
	@studyWindowWhereClause
	;

CREATE TABLE #subgroup_person (
	subgroup_id BIGINT NOT NULL,
	subject_id BIGINT NOT NULL,
	start_date DATE NOT NULL
	);

@subgroupQueries

/*
3) create table to store era-fied excluded at-risk periods
  UNION all periods that don't require erafying
  with era-fy records that require it
*/
-- find excluded time from outcome cohorts and exclusion cohorts
-- note, clean window added to event end date
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT or1.outcome_id,
	oc1.subject_id,
	DATEADD(dd, 1, oc1.cohort_start_date) AS cohort_start_date,
	DATEADD(dd, or1.clean_window, oc1.cohort_end_date) AS cohort_end_date
INTO #excluded_tar_cohort
FROM @outcome_cohort_database_schema.@outcome_cohort_table oc1
INNER JOIN (
	SELECT outcome_id,
		outcome_cohort_definition_id,
		clean_window
	FROM #outcome_ref
	) or1 ON oc1.cohort_definition_id = or1.outcome_cohort_definition_id
WHERE DATEADD(dd, or1.clean_window, oc1.cohort_end_date) >= DATEADD(dd, 1, oc1.cohort_end_date)

UNION ALL

SELECT or1.outcome_id,
	c1.subject_id,
	c1.cohort_start_date,
	c1.cohort_end_date
FROM @outcome_cohort_database_schema.@outcome_cohort_table c1
INNER JOIN (
	SELECT outcome_id,
		excluded_cohort_definition_id
	FROM #outcome_ref
	) or1 ON c1.cohort_definition_id = or1.excluded_cohort_definition_id;

--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT te1.cohort_definition_id AS target_cohort_definition_id,
	te1.tar_id,
	ec1.outcome_id,
	ec1.subject_id,
	CASE 
		WHEN ec1.cohort_start_date > te1.start_date
			THEN ec1.cohort_start_date
		ELSE te1.start_date
		END AS start_date,
	CASE 
		WHEN ec1.cohort_end_date < te1.end_date
			THEN ec1.cohort_end_date
		ELSE te1.end_date
		END AS end_date
INTO #exc_TTAR_o
FROM #TTAR_erafied te1
INNER JOIN #excluded_tar_cohort ec1 ON te1.subject_id = ec1.subject_id
	AND ec1.cohort_start_date <= te1.end_date
	AND ec1.cohort_end_date >= te1.start_date;

--find the records that need to be era-fied
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT t1.target_cohort_definition_id,
	t1.tar_id,
	t1.outcome_id,
	t1.subject_id,
	t1.start_date,
	t1.end_date
INTO #exc_TTAR_o_to_erafy
FROM #exc_TTAR_o t1
INNER JOIN #exc_TTAR_o t2 ON t1.target_cohort_definition_id = t2.target_cohort_definition_id
	AND t1.tar_id = t2.tar_id
	AND t1.outcome_id = t2.outcome_id
	AND t1.subject_id = t2.subject_id
	AND t1.start_date < t2.end_date
	AND t1.end_date > t2.start_date
	AND (
		t1.start_date <> t2.start_date
		OR t1.end_date <> t2.end_date
		);

--era-building script for the 'exc_TTAR_o_to_erafy ' records
--insert records from era-building script into #TTAR_erafied
--HINT DISTRIBUTE_ON_KEY(subject_id)
WITH cteEndDates (
	target_cohort_definition_id,
	tar_id,
	outcome_id,
	subject_id,
	end_date
	)
AS (
	SELECT target_cohort_definition_id,
		tar_id,
		outcome_id,
		subject_id,
		event_date AS end_date
	FROM (
		SELECT target_cohort_definition_id,
			tar_id,
			outcome_id,
			subject_id,
			event_date,
			SUM(event_type) OVER (
				PARTITION BY target_cohort_definition_id,
				tar_id,
				outcome_id,
				subject_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
				) AS interval_status
		FROM (
			SELECT target_cohort_definition_id,
				tar_id,
				outcome_id,
				subject_id,
				start_date AS event_date,
				- 1 AS event_type
			FROM #exc_TTAR_o_to_erafy
			
			UNION ALL
			
			SELECT target_cohort_definition_id,
				tar_id,
				outcome_id,
				subject_id,
				end_date AS event_date,
				1 AS event_type
			FROM #exc_TTAR_o_to_erafy
			) RAWDATA
		) e
	WHERE interval_status = 0
	),
cteEnds (
	target_cohort_definition_id,
	tar_id,
	outcome_id,
	subject_id,
	start_date,
	end_date
	)
AS (
	SELECT c.target_cohort_definition_id,
		c.tar_id,
		c.outcome_id,
		c.subject_id,
		c.start_date,
		MIN(e.end_date) AS end_date
	FROM #exc_TTAR_o_to_erafy c
	INNER JOIN cteEndDates e ON c.subject_id = e.subject_id
		AND c.target_cohort_definition_id = e.target_cohort_definition_id
		AND c.tar_id = e.tar_id
		AND c.outcome_id = e.outcome_id
		AND e.end_date >= c.start_date
	GROUP BY c.target_cohort_definition_id,
		c.tar_id,
		c.outcome_id,
		c.subject_id,
		c.start_date
	)
SELECT target_cohort_definition_id,
	tar_id,
	outcome_id,
	subject_id,
	min(start_date) AS start_date,
	end_date
INTO #ex_TTAR_o_overlaps
FROM cteEnds
GROUP BY target_cohort_definition_id,
	tar_id,
	outcome_id,
	subject_id,
	end_date;

--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT target_cohort_definition_id,
	tar_id,
	outcome_id,
	subject_id,
	start_date,
	end_date
INTO #exc_TTAR_o_erafied
FROM #ex_TTAR_o_overlaps

UNION ALL

--records that were already erafied and just need to be brought over directly
SELECT DISTINCT t1.target_cohort_definition_id,
	t1.tar_id,
	t1.outcome_id,
	t1.subject_id,
	t1.start_date,
	t1.end_date
FROM #exc_TTAR_o t1
LEFT JOIN #exc_TTAR_o t2 ON t1.target_cohort_definition_id = t2.target_cohort_definition_id
	AND t1.tar_id = t2.tar_id
	AND t1.outcome_id = t2.outcome_id
	AND t1.subject_id = t2.subject_id
	AND t1.start_date < t2.end_date
	AND t1.end_date > t2.start_date
	AND (
		t1.start_date <> t2.start_date
		OR t1.end_date <> t2.end_date
		)
WHERE t2.subject_id IS NULL;

-- 4) calculate pre-exclude outcomes and outcomes 
-- calculate pe_outcomes and outcomes by T, TAR, O, Subject, TAR start
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT t1.cohort_definition_id AS target_cohort_definition_id,
	t1.tar_id,
	t1.subject_id,
	t1.start_date,
	o1.outcome_id,
	count_big(o1.subject_id) AS pe_outcomes,
	SUM(CASE 
			WHEN eo.tar_id IS NULL
				THEN 1
			ELSE 0
			END) AS num_outcomes
INTO #outcome_smry
FROM #TTAR_erafied t1
INNER JOIN (
	SELECT oref.outcome_id,
		oc.subject_id,
		oc.cohort_start_date
	FROM @outcome_cohort_database_schema.@outcome_cohort_table oc
	INNER JOIN #outcome_ref oref ON oc.cohort_definition_id = oref.outcome_cohort_definition_id
	) o1 ON t1.subject_id = o1.subject_id
	AND t1.start_date <= o1.cohort_start_date
	AND t1.end_date >= o1.cohort_start_date
LEFT JOIN #exc_TTAR_o_erafied eo ON t1.cohort_definition_id = eo.target_cohort_definition_id
	AND t1.tar_id = eo.tar_id
	AND o1.outcome_id = eo.outcome_id
	AND o1.subject_id = eo.subject_id
	AND eo.start_date <= o1.cohort_start_date
	AND eo.end_date >= o1.cohort_start_date
GROUP BY t1.cohort_definition_id,
	t1.tar_id,
	t1.subject_id,
	t1.start_date,
	o1.outcome_id;

-- 5) calculate exclusion time per T/O/TAR/Subject/start_date
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT t1.cohort_definition_id AS target_cohort_definition_id,
	t1.tar_id,
	t1.subject_id,
	t1.start_date,
	et1.outcome_id,
	sum(datediff(dd, et1.start_date, et1.end_date) + 1) AS person_days
INTO #excluded_person_days
FROM #TTAR_erafied t1
INNER JOIN #exc_TTAR_o_erafied et1 ON t1.cohort_definition_id = et1.target_cohort_definition_id
	AND t1.tar_id = et1.tar_id
	AND t1.subject_id = et1.subject_id
	AND t1.start_date <= et1.start_date
	AND t1.end_date >= et1.end_date
GROUP BY t1.cohort_definition_id,
	t1.tar_id,
	t1.subject_id,
	t1.start_date,
	et1.outcome_id;

/*
6) generate raw result table with T/O/TAR/subject_id,start_date, pe_at_risk (datediff(d,start,end), at_risk (pe_at_risk - exclusion time), pe_outcomes, outcomes
   and attach age/gender/year columns
*/
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT t1.target_cohort_definition_id,
	o1.outcome_id,
	t1.tar_id,
	t1.subject_id,
	t1.start_date,
	ag.age_id,
	t1.gender_id,
	t1.start_year,
	datediff(dd, t1.start_date, t1.end_date) + 1 AS pe_person_days,
	datediff(dd, t1.start_date, t1.end_date) + 1 - COALESCE(te1.person_days, 0) AS person_days,
	COALESCE(os1.pe_outcomes, 0) AS pe_outcomes,
	COALESCE(os1.num_outcomes, 0) AS outcomes
INTO #incidence_raw
FROM (
	SELECT te.cohort_definition_id AS target_cohort_definition_id,
		te.tar_id,
		te.subject_id,
		te.start_date,
		te.end_date,
		YEAR(te.start_date) - p.year_of_birth AS age,
		p.gender_concept_id AS gender_id,
		YEAR(te.start_date) AS start_year
	FROM #TTAR_erafied te
	INNER JOIN @cdm_database_schema.person p ON te.subject_id = p.person_id
	) t1
CROSS JOIN (
	SELECT outcome_id
	FROM #outcome_ref
	) o1
LEFT JOIN #excluded_person_days te1 ON t1.target_cohort_definition_id = te1.target_cohort_definition_id
	AND t1.tar_id = te1.tar_id
	AND t1.subject_id = te1.subject_id
	AND t1.start_date = te1.start_date
	AND o1.outcome_id = te1.outcome_id
LEFT JOIN #outcome_smry os1 ON t1.target_cohort_definition_id = os1.target_cohort_definition_id
	AND t1.tar_id = os1.tar_id
	AND t1.subject_id = os1.subject_id
	AND t1.start_date = os1.start_date
	AND o1.outcome_id = os1.outcome_id
LEFT JOIN #age_group ag ON t1.age >= COALESCE(ag.min_age, - 999)
	AND t1.age < COALESCE(ag.max_age, 999);

-- 7) Create analysis_ref to produce each T/O/TAR/S combo
SELECT t1.target_cohort_definition_id,
	t1.target_name,
	tar1.tar_id,
	tar1.risk_window_start,
	tar1.start_anchor,
	tar1.risk_window_end,
	tar1.end_anchor,
	s1.subgroup_id,
	s1.subgroup_name,
	o1.outcome_id,
	o1.outcome_cohort_definition_id,
	o1.outcome_name,
	o1.clean_window
INTO #tscotar_ref
FROM (
	SELECT target_cohort_definition_id,
		target_name
	FROM #target_ref
	) t1,
	(
		SELECT tar_id,
			risk_window_start,
			start_anchor,
			risk_window_end,
			end_anchor
		FROM #tar_ref
		) tar1,
	(
		SELECT subgroup_id,
			subgroup_name
		FROM #subgroup_ref
		) s1,
	(
		SELECT outcome_id,
			outcome_cohort_definition_id,
			outcome_name,
			clean_window
		FROM #outcome_ref
		) o1;

-- 8) perform rollup to calculate IR / IP at the T/O/TAR/S/[age|gender|year] level for 'all' and each subgroup
-- and aggregate to the selected levels
WITH incidence_w_subgroup (
	subgroup_id,
	target_cohort_definition_id,
	outcome_id,
	tar_id,
	subject_id,
	age_id,
	gender_id,
	start_year,
	pe_person_days,
	person_days,
	pe_outcomes,
	outcomes
	)
AS (
	-- the 'all' group
	SELECT CAST(0 AS INT) AS subgroup_id,
		ir.target_cohort_definition_id,
		ir.outcome_id,
		ir.tar_id,
		ir.subject_id,
		ir.age_id,
		ir.gender_id,
		ir.start_year,
		ir.pe_person_days,
		ir.person_days,
		ir.pe_outcomes,
		ir.outcomes
	FROM #incidence_raw ir
	
	UNION ALL
	
	-- select the individual subgroup members using the subgroup_person table
	SELECT s.subgroup_id AS subgroup_id,
		ir.target_cohort_definition_id,
		ir.outcome_id,
		ir.tar_id,
		ir.subject_id,
		ir.age_id,
		ir.gender_id,
		ir.start_year,
		ir.pe_person_days,
		ir.person_days,
		ir.pe_outcomes,
		ir.outcomes
	FROM #incidence_raw ir
	INNER JOIN #subgroup_person s ON ir.subject_id = s.subject_id
		AND ir.start_date = s.start_date
	)
SELECT target_cohort_definition_id,
	tar_id,
	subgroup_id,
	outcome_id,
	age_id,
	gender_id,
	start_year,
	persons_at_risk_pe,
	persons_at_risk,
	person_days_pe,
	person_days,
	person_outcomes_pe,
	person_outcomes,
	outcomes_pe,
	outcomes
INTO #incidence_subgroups
FROM (
	SELECT irs.target_cohort_definition_id,
		irs.tar_id,
		irs.subgroup_id,
		irs.outcome_id,
		CAST(NULL AS INT) AS age_id,
		CAST(NULL AS INT) AS gender_id,
		CAST(NULL AS INT) AS start_year,
		count_big(DISTINCT irs.subject_id) AS persons_at_risk_pe,
		count_big(DISTINCT CASE 
				WHEN irs.person_days > 0
					THEN irs.subject_id
				END) AS persons_at_risk,
		sum(CAST(irs.pe_person_days AS BIGINT)) AS person_days_pe,
		sum(CAST(irs.person_days AS BIGINT)) AS person_days,
		count_big(DISTINCT CASE 
				WHEN irs.pe_outcomes > 0
					THEN irs.subject_id
				END) AS person_outcomes_pe,
		count_big(DISTINCT CASE 
				WHEN irs.outcomes > 0
					THEN irs.subject_id
				END) AS person_outcomes,
		sum(CAST(irs.pe_outcomes AS BIGINT)) AS outcomes_pe,
		sum(CAST(irs.outcomes AS BIGINT)) AS outcomes
	FROM incidence_w_subgroup irs
	GROUP BY irs.target_cohort_definition_id,
		irs.tar_id,
		irs.subgroup_id,
		irs.outcome_id @strataQueries
	) IR;

SELECT --CAST(@ref_id AS INT) AS ref_id,
	--'@sourceName' AS source_name,
	tref.target_cohort_definition_id,
	tref.target_name,
	tref.tar_id,
	tref.start_anchor,
	tref.risk_window_start,
	tref.end_anchor,
	tref.risk_window_end,
	tref.subgroup_id,
	tref.subgroup_name,
	tref.outcome_id,
	tref.outcome_cohort_definition_id,
	tref.outcome_name,
	tref.clean_window,
	irs.age_id,
	ag.group_name,
	irs.gender_id,
	c.concept_name AS gender_name,
	irs.start_year,
	COALESCE(irs.persons_at_risk_pe, 0) AS persons_at_risk_pe,
	COALESCE(irs.persons_at_risk, 0) AS persons_at_risk,
	COALESCE(irs.person_days_pe, 0) AS person_days_pe,
	COALESCE(irs.person_days, 0) AS person_days,
	COALESCE(irs.person_outcomes_pe, 0) AS person_outcomes_pe,
	COALESCE(irs.person_outcomes, 0) AS person_outcomes,
	COALESCE(irs.outcomes_pe, 0) AS outcomes_pe,
	COALESCE(irs.outcomes, 0) AS outcomes,
	CASE 
		WHEN COALESCE(irs.persons_at_risk, 0) > 0
			THEN (100.0 * CAST(COALESCE(irs.person_outcomes, 0) AS FLOAT) / (CAST(COALESCE(irs.persons_at_risk, 0) AS FLOAT)))
		END AS incidence_proportion_p100p,
	CASE 
		WHEN COALESCE(irs.person_days, 0) > 0
			THEN (100.0 * CAST(COALESCE(irs.outcomes, 0) AS FLOAT) / (CAST(COALESCE(irs.person_days, 0) AS FLOAT) / 365.25))
		END AS incidence_rate_p100py
INTO #incidence_summary
FROM #tscotar_ref tref
LEFT JOIN #incidence_subgroups irs ON tref.target_cohort_definition_id = irs.target_cohort_definition_id
	AND tref.tar_id = irs.tar_id
	AND tref.subgroup_id = irs.subgroup_id
	AND tref.outcome_id = irs.outcome_id
LEFT JOIN #age_group ag ON ag.age_id = irs.age_id
LEFT JOIN @cdm_database_schema.concept c ON c.concept_id = irs.gender_id;

-- CLEANUP TEMP TABLES
DROP TABLE #TTAR;

DROP TABLE #TTAR_to_erafy;

DROP TABLE #TTAR_era_overlaps;

DROP TABLE #TTAR_erafied;

DROP TABLE #subgroup_person;

DROP TABLE #excluded_tar_cohort;

DROP TABLE #exc_TTAR_o;

DROP TABLE #ex_TTAR_o_overlaps;

DROP TABLE #exc_TTAR_o_to_erafy;

DROP TABLE #exc_TTAR_o_erafied;

DROP TABLE #outcome_smry;

DROP TABLE #excluded_person_days;

DROP TABLE #incidence_raw;

DROP TABLE #tscotar_ref;

DROP TABLE #incidence_subgroups;
