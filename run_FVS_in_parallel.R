### Note: This code is meant to be paired with one of the scripts used to generate the .key files. The present version matches the generate_key_files_for_multiple_kcps_and_variants.R script
# You may need to adjust the code to generate the variant_kcp dataframe to then reference in the future_pmap call 

library(tidyverse) #For data manipulation
library(RSQLite) #For working with sqlite databases
library(terra) #For raster and vector geospatial operations
library(data.table) #Faster data format, works with default R dataframe functions
library(furrr) #For parallelization
library(withr) #For parallelization - used to temporarily change the working directory for each worker

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
      fvsProgram = paste0("FVS", variant, ".dll"), #the dll files have lowercase variant labels
      bin = fvs_bin
    )
    
    # Set the command line (keyword file)
    print(paste0("--keywordfile=", kcp, "_", toupper(variant), ".key")) #the database has uppercase variant labels, which are used to generate the .key files
    #For FSim scenario runs
    # print(paste0("--keywordfile=", kcp, "_", flame_length, "_", toupper(variant), ".key")) #adjust path below similarly
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
  # Return useful info:
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

##For FSim scenarios
# variants <- "ec"
# kcps <- "simfire_hardcoded"
# flame_lengths <- c(1, 3, 5, 7, 10, 20)
# runs <- expand.grid(variant = variants, kcp = kcps, flame_length = flame_lengths)

#Plan for parallel execution
n_runs <- nrow(variant_kcp)
##NOTE: if n_runs exceeds the number of logical processors on your machine, set the workers equal to the number of logical processors. Setting the number of workers too high may crash R.
future::plan(multisession, workers = n_runs)

#Run FVS in parallel for all combinations
furrr::future_pmap(
  list(
    variant = variant_kcp$variant,
    kcp = variant_kcp$kcp
    #flame_length=runs$flame_length #Use this for FSim scenario runs with the above code to create the runs grid
  ),
  runFVS,
  RunDirectory = RunDirectory,
  fvs_bin = fvs_bin
)

plan(sequential)
