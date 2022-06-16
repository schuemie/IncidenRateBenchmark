# This code was used to select the target and outcome concept IDs
library(DatabaseConnector)
library(dplyr)

connectionDetails <- createConnectionDetails(dbms = "redshift",
                                             connectionString = keyring::key_get("redShiftConnectionStringOhdaCcae"),
                                             user = keyring::key_get("redShiftUserName"),
                                             password = keyring::key_get("redShiftPassword"))
cdmDatabaseSchema <- "cdm_truven_ccae_v2008"

# Find conditions and drugs ----------------------------------------------------
connection <- connect(connectionDetails)

# The table 1 of FeatureExtraction has a nice generic list of outcomes of interest, so we'll use that:
table1Specs <- FeatureExtraction::getDefaultTable1Specifications() %>%
  filter(grepl("Medical history",.data$label))
getConceptIds <- function(string) {
  return(round(as.numeric(strsplit(string, ",")[[1]]) / 1000))
}
conceptIds <- do.call(c, lapply(table1Specs$covariateIds, getConceptIds))

sql <- "SELECT concept_id,
  concept_name
FROM @cdm_database_schema.concept
WHERE concept_id IN (@concept_ids);"
prevalentConditions <- renderTranslateQuerySql(connection = connection,
                                               sql = sql,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               concept_ids = conceptIds,
                                               snakeCaseToCamelCase = TRUE)

# Our target cohort will be the most prevalent drug:
sql <- "SELECT TOP 1 concept_id,
  concept_name
FROM @cdm_database_schema.drug_era
INNER JOIN @cdm_database_schema.concept
  ON drug_concept_id = concept_id
GROUP BY concept_id,
  concept_name
ORDER BY -COUNT(*);"
prevalentDrugs <- renderTranslateQuerySql(connection = connection,
                                          sql = sql,
                                          cdm_database_schema = cdmDatabaseSchema,
                                          snakeCaseToCamelCase = TRUE)

disconnect(connection)

# Combine and save -------------------------------------------------------------
referenceSet <- bind_rows(prevalentConditions %>%
                            mutate(type = "outcome"),
                          prevalentDrugs %>%
                            mutate(type = "target"))
readr::write_csv(referenceSet, "ReferenceSet.csv")
