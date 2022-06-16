# Main script for running the benchmark
library(dplyr)

# Define connection to your database -------------------------------------------
# Make changes here as required for your local environment.
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringOhdaCcae"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))
cdmDatabaseSchema <- "cdm_truven_ccae_v2008"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "ir_benchmark"

options(sqlRenderTempEmulationSchema = NULL)
options(andromedaTempFolder = "s:/andromedaTemp")


# Create cohorts and cohort table ----------------------------------------------
source("CreateCohorts.R")
createCohorts(connectionDetails = connectionDetails,
              cdmDatabaseSchema = cdmDatabaseSchema,
              cohortDatabaseSchema = cohortDatabaseSchema,
              cohortTable = cohortTable)

# Run benchmark using SQL ------------------------------------------------------
source("IncidenceRatesUsingSql.R")
incidenceRates <- computeIncidenceRatesUsingSql(connectionDetails = connectionDetails,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortTable = cohortTable)

# CCAE: Computing incidence rates took 1.66 hours
# readr::write_csv(incidenceRates, "incidenceRatesCcaeSql.csv")

# Run benchmark using R --------------------------------------------------------
source("IncidenceRatesUsingR.R")
incidenceRates <- computeIncidenceRatesUsingR(connectionDetails = connectionDetails,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              cohortTable = cohortTable)
