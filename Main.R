# Main script for running the benchmark
library(dplyr)

# Define connection to your database -------------------------------------------
# Make changes here as required for your local environment.

# Details for CCAE on RedShift at JnJ:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringOhdaCcae"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))
cdmDatabaseSchema <- "cdm_truven_ccae_v2008"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "ir_benchmark"
options(sqlRenderTempEmulationSchema = NULL)
options(andromedaTempFolder = "s:/andromedaTemp")

# Details for Synpuf on Postgres at JnJ:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = paste(keyring::key_get("postgresServer"), keyring::key_get("postgresDatabase"), sep = "/"),
                                                                user = keyring::key_get("postgresUser"),
                                                                password = keyring::key_get("postgresPassword"),
                                                                port = keyring::key_get("postgresPort"))
cdmDatabaseSchema <- "synpuf"
cohortDatabaseSchema <- "scratch"
cohortTable <- "ir_benchmark"
options(sqlRenderTempEmulationSchema = NULL)
options(andromedaTempFolder = "d:/andromedaTemp")

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

# CCAE on RedShift: Computing incidence rates took 1.66 hours
# readr::write_csv(incidenceRates, "incidenceRatesCcaeSql.csv")

# Synpuf on Postgres: Computing incidence rates took 7.71 mins

# Run benchmark using R --------------------------------------------------------
source("IncidenceRatesUsingR.R")
incidenceRates <- computeIncidenceRatesUsingR(connectionDetails = connectionDetails,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              cohortTable = cohortTable)
# CCAE on RedShift, downloading relevant data only: Computing incidence rates took 3.09 hours

# Synpuf on Postgres, downloading relevant data only: Computing incidence rates took 6.49 mins

