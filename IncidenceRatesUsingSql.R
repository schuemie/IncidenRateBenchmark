require(dplyr)

computeIncidenceRatesUsingSql <- function(connectionDetails, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable) {
  startTime <- Sys.time()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  uploadSettingsTempTables(connection)
  incidenceRates <- runIncidenRatesAnalysisSql(connection, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable)
  dropSettingsTempTables(connection)

  delta <- Sys.time() - startTime
  message(paste("Computing incidence rates took", signif(delta, 3), attr(delta, "units")))

  return(incidenceRates)
}

uploadSettingsTempTables <- function(connection) {
  referenceSet <- readr::read_csv("ReferenceSet.csv", show_col_types = FALSE)

  message("Uploading target reference")
  targetRef <- referenceSet %>%
    filter(.data$type == "target") %>%
    select(targetCohortDefinitionId = .data$conceptId,
           targetName = .data$conceptName)

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#target_ref",
                                 data = targetRef,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 camelCaseToSnakeCase = TRUE)

  message("Uploading outcome reference")
  outcomeRef <- referenceSet %>%
    filter(.data$type == "outcome") %>%
    select(outcomeCohortDefinitionId = .data$conceptId,
           outcomeName = .data$conceptName) %>%
    mutate(outcomeId = row_number(),
           cleanWindow = 30,
           excludedCohortDefinitionId = 0)

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#outcome_ref",
                                 data = outcomeRef,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 camelCaseToSnakeCase = TRUE)

  message("Uploading time-at-risk reference")
  tarRef <- tibble(tarId = 1,
                   startAnchor = "cohort start",
                   riskWindowStart = 0,
                   endAnchor = "cohort end",
                   riskWindowEnd = 0)

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#tar_ref",
                                 data = tarRef,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 camelCaseToSnakeCase = TRUE)

  message("Uploading age groups")
  ageGroup <- tibble(ageId = 1:21,
                     minAge = 0:20 * 5,
                     maxAge = c(1:20 * 5 - 1, 999)) %>%
    mutate(groupName = sprintf("Age %d-%d", .data$minAge, .data$maxAge))

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#age_group",
                                 data = ageGroup,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 camelCaseToSnakeCase = TRUE)

  message("Uploading subgroup reference")
  subgroupRef <- tibble(subgroupId = 0,
                        subgroupName = "All")

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#subgroup_ref",
                                 data = subgroupRef,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 camelCaseToSnakeCase = TRUE)
}

runIncidenRatesAnalysisSql <- function(connection, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable) {
  # Subquery for age, sex, and calendar year stratification:
  strataQueries <- "UNION ALL

  SELECT irs.target_cohort_definition_id,
		irs.tar_id,
		irs.subgroup_id,
		irs.outcome_id,
    irs.age_id,
    irs.gender_id,
    irs.start_year,
		COUNT_BIG(distinct irs.subject_id) AS persons_at_risk_pe,
		COUNT_BIG(distinct CASE WHEN irs.person_days > 0 THEN irs.subject_id END) AS persons_at_risk,
		SUM(CAST(irs.pe_person_days AS bigint)) AS person_days_pe,
		SUM(CAST(irs.person_days AS bigint)) AS person_days,
		COUNT_BIG(distinct CASE WHEN irs.pe_outcomes > 0 THEN irs.subject_id END) AS person_outcomes_pe,
		COUNT_BIG(distinct CASE WHEN irs.outcomes > 0 THEN irs.subject_id END) AS person_outcomes,
		SUM(CAST(irs.pe_outcomes AS bigint)) AS outcomes_pe,
		SUM(CAST(irs.outcomes AS bigint)) AS outcomes
	FROM incidence_w_subgroup irs
	GROUP BY irs.target_cohort_definition_id,
	  irs.tar_id,
	  irs.subgroup_id,
	  irs.outcome_id,
	  irs.age_id,
    irs.gender_id,
    irs.start_year"
  sql <- SqlRender::readSql("IncidenceRates.sql")
  message("Computing incidence rates")
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               target_cohort_database_schema = cohortDatabaseSchema,
                                               target_cohort_table = cohortTable,
                                               outcome_cohort_database_schema = cohortDatabaseSchema,
                                               outcome_cohort_table = cohortTable,
                                               subgroupQueries = "",
                                               studyWindowWhereClause = "",
                                               tarEndDateExpression = "end_date",
                                               strataQueries = strataQueries)
  sql <- "SELECT *
  FROM #incidence_summary;"
  incidenceRates <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                               sql = sql,
                                                               snakeCaseToCamelCase = TRUE)

  sql <- "DROP TABLE #incidence_summary;"
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql)
  return(incidenceRates)
}

dropSettingsTempTables <- function(connection) {
  sql <- "DROP TABLE #target_ref;
  DROP TABLE #tar_ref;
  DROP TABLE #outcome_ref;
  DROP TABLE #subgroup_ref;
  DROP TABLE #age_group;"
  DatabaseConnector::renderTranslateExecuteSql(connection, sql, reportOverallTime = FALSE, progressBar = FALSE)
}
