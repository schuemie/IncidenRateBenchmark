require(dplyr)

computeIncidenceRatesUsingR <- function(connectionDetails, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable) {
  startTime <- Sys.time()

  cohorts <- downloadCohorts(connectionDetails, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable)

  delta <- Sys.time() - startTime
  message(paste("Computing incidence rates took", signif(delta, 3), attr(delta, "units")))
}


downloadCohorts <- function(connectionDetails, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable) {

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  referenceSet <- readr::read_csv("ReferenceSet.csv", show_col_types = FALSE)
  targetCohortIds <- referenceSet %>%
    filter(.data$type == "target") %>%
    pull(.data$conceptId)
  outcomeCohortIds <- referenceSet %>%
    filter(.data$type == "outcome") %>%
    pull(.data$conceptId)

  cohorts <- Andromeda::andromeda()

  sql <- "SELECT *
  FROM @cohort_database_schema.@cohort_table
  WHERE cohort_definition_id IN (@cohort_ids);"
  message("Downloading target cohorts")
  DatabaseConnector::renderTranslateQuerySqlToAndromeda(connection = connection,
                                                        sql = sql,
                                                        andromeda = cohorts,
                                                        andromedaTableName = "targetCohorts",
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_table = cohortTable,
                                                        cohort_ids = targetCohortIds,
                                                        snakeCaseToCamelCase = TRUE,
                                                        integer64AsNumeric = FALSE)

  message("Downloading outcome cohorts")
  DatabaseConnector::renderTranslateQuerySqlToAndromeda(connection = connection,
                                                        sql = sql,
                                                        andromeda = cohorts,
                                                        andromedaTableName = "outcomeCohorts",
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_table = cohortTable,
                                                        cohort_ids = outcomeCohortIds,
                                                        snakeCaseToCamelCase = TRUE,
                                                        integer64AsNumeric = FALSE)

  sql <- "SELECT *
  FROM @cdm_database_schema.observation_period
  WHERE person_id in (
    SELECT DISTINCT subject_id
    FROM @cohort_database_schema.@cohort_table
    WHERE cohort_definition_id IN (@cohort_ids));"
  message("Downloading observation periods for target cohorts")
  DatabaseConnector::renderTranslateQuerySqlToAndromeda(connection = connection,
                                                        sql = sql,
                                                        andromeda = cohorts,
                                                        andromedaTableName = "observationPeriod",
                                                        cdm_database_schema = cdmDatabaseSchema,
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_table = cohortTable,
                                                        cohort_ids = targetCohortIds,
                                                        snakeCaseToCamelCase = TRUE,
                                                        integer64AsNumeric = FALSE)
  return(cohorts)
}
