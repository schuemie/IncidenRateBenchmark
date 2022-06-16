Incidence rate benchmark
========================

This R project aims to benchmark different approaches to computing incidence rates. A toy example is defined, and various approaches are executed on the example in various databases. Time taken to complete, as well as agreement in produced incidence rates is computed.

# Toy problem definition

The toy example consists of computing incidence rates for a set of outcome cohorts within a set of target cohorts. An overview of these cohorts can be found in [ReferenceSet.csv](ReferenceSet.csv).

## Outcome cohorts

A set of 10 outcome cohorts have been predefined. These outcomes can be **recurring**, meaning that the same person can experience the same outcome more than once. 

## Target cohorts

A set of 10 target cohorts have been defined. These cohorts can be **recurring**, meaning that the same person can enter the same cohort more than once.

## Incidence rate computation

The incidence rate is computed as the number of starts of the outcome cohort divided by the time at risk. The time at risk is defined at the time a person is in the target cohort, and is eligible to start the outcome. A person is ineligible to start the outcome when still experiencing the outcome (i.e. between the cohort start and end date), and in the 30 days following the outcome end (the so-called '**clean window**').

The incidence rates are computed for all outcomes within all target cohorts, so in total 10 x 10 = 100 incidence rates will be computed.

### Age, sex, and calendar year strata

In addition to the overall incidence rate, the incidence rates are also be computed per age, sex, and calendar year strata. Age is divided in 5-year age bins, with a single bin for ages >= 100.

# Benchmark execution

The [Main.r](Main.R) script contains the code for executing the benchmark. It first creates the cohort table, and instantiates the cohorts. Then, the various methods for computing incidence rates are executed.
