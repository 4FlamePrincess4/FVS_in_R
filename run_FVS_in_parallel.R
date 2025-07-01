### Note: This code is meant to be paired with one of the scripts used to generate the .key files. The present version matches the generate_key_files_for_multiple_kcps_and_variants.R script
# You may need to adjust the code to generate the variant_kcp dataframe to then reference in the future_pmap call 

library(tidyverse)
library(RSQLite)
library(data.table)
library(stringr)
library(furrr)
library(withr)
#Install the rFVS package
install.packages("remotes")
remotes::install_github("SilviaTerra/rFVS")
library(rFVS)

setwd("D:/WFSETP/FVS_Training/Run_FVS")
wd <- getwd()

#Set the FVS executable folder
fvs_bin = "C:/FVS/FVSSoftware/FVSbin"

#---Set location of TMFM sqlite databases and read in data----------------------
TMFM2020_dir_path <- "D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/TMFM_2020_OkaWen_Databases"

okawen_dbs <- list.files(TMFM2020_dir_path,
                         full.names=TRUE)
#We only need to run this for the East Cascades variant
EC_dbs <- okawen_dbs[1]

# Create a dir for this run.
RunDirectory <- "insert_run_directory_path_here"

#Create the output database file path from Rachel: "database MUST exist before running FVS"
outputDatabase <- paste0("./outputs/", run_name, ".db")

#Make an outputs directory
dir.create(paste0(RunDirectory, "/outputs"))

runFVS <- function(variant, kcp, RunDirectory, fvs_bin) {
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Set the command line (keyword file)
    print(paste0("--keywordfile=", kcp, "_", toupper(variant), ".key"))
    rFVS::fvsSetCmdLine(
      cl = paste0("--keywordfile=", kcp, "_", toupper(variant), ".key"),
      PACKAGE = paste0("FVS", variant)
    )
    
    # Run FVS until done — IMPORTANT: pass PACKAGE here too!
    retCode <- 0
    while (retCode == 0) {
      retCode <- rFVS::fvsRun(PACKAGE = paste0("FVS", variant))
    }
  })
  # Explicitly return useful info:
  list(
    variant = variant,
    kcp = kcp,
    status = retCode
  )
}

#List the KCP files
kcp_dir <- "D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/OkaWen_kcps_v2_Eireann_feedback"
kcp_paths <- list.files(kcp_dir)

#variants <- c("ec", "ie", "pn", "wc")
variants <- "ec"
kcps <- stringr::str_sub(kcp_paths, end = -5) 
variant_kcp <- expand.grid(variant = variants, kcp = kcps)

#Plan for parallel execution
n_runs <- nrow(variant_kcp)
##NOTE: if n_runs exceeds the number of logical processors on your machine, set the workers equal to the number of logical processors. Setting the number of workers too high may crash R.
future::plan(multisession, workers = n_runs)

#Run FVS in parallel for all combinations
furrr::future_pmap(
  list(
    variant = variant_kcp$variant,
    kcp = variant_kcp$kcp
  ),
  runFVS,
  RunDirectory = RunDirectory,
  fvs_bin = fvs_bin
)

plan(sequential)
