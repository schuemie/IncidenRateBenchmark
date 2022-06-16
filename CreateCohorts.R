# Provides a function for generating the cohorts used in the benchmark
# For simplicity, cohorts are one-on-one copies of condition eras and drug eras
require(dplyr)

createCohorts <- function(connectionDetails, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  referenceSet <- readr::read_csv("ReferenceSet.csv", show_col_types = FALSE)
  outcomeConceptIds <- referenceSet %>%
    filter(.data$type == "outcome") %>%
    pull(.data$conceptId)
  targetConceptIds <- referenceSet %>%
    filter(.data$type == "target") %>%
    pull(.data$conceptId)

  sql <- "DROP TABLE IF EXISTS @cohort_database_schema.@cohort_table;

  --HINT DISTRIBUTE_ON_KEY(subject_id)
  SELECT *
  INTO @cohort_database_schema.@cohort_table
  FROM (
    SELECT person_id AS subject_id,
      condition_concept_id AS cohort_definition_id,
      condition_era_start_date AS cohort_start_date,
      condition_era_end_date AS cohort_end_date
    FROM @cdm_database_schema.condition_era
    WHERE condition_concept_id IN (@outcome_concept_ids)

    UNION ALL

    SELECT person_id AS subject_id,
      drug_concept_id AS cohort_definition_id,
      drug_era_start_date AS cohort_start_date,
      drug_era_end_date AS cohort_end_date
    FROM @cdm_database_schema.drug_era
    WHERE drug_concept_id IN (@target_concept_ids)
  ) tmp;"
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               cohort_database_schema = cohortDatabaseSchema,
                                               cohort_table = cohortTable,
                                               outcome_concept_ids = outcomeConceptIds,
                                               target_concept_ids = targetConceptIds)

 }
