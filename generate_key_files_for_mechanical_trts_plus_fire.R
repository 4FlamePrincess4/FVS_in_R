library(tidyverse) #For data manipulation
library(RSQLite) #For working with sqlite databases
library(terra) #For raster and vector geospatial operations
library(data.table) #Faster data format, works with default R dataframe functions
library(foreign) #For reading in .dbf files
library(furrr) #For parallelization
library(withr) #For parallelization - used to temporarily change the working directory for each worker

#Install the rFVS package
install.packages("remotes")
remotes::install_github("SilviaTerra/rFVS")
library(rFVS)

setwd("D:/WFSETP/Scenario_Development/Baseline_2020_FVS")
wd <- getwd()

#Set the FVS executable folder
fvs_bin = "C:/FVS/FVSSoftware/FVSbin"

#---Set location of TMFM sqlite databases and read in data----------------------
TMFM2020_dir_path <- "D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/TMFM_2020_OkaWen_Databases"

okawen_dbs <- list.files(TMFM2020_dir_path,
                         full.names=TRUE)
#We only need to run this for the East Cascades variant
EC_dbs <- okawen_dbs[1]

#Name the FVS run
run_name <- str_c("FSim_scenarios_test_",
                  strftime(Sys.Date(), "%d%b%y"),
                  "_", strftime(Sys.time(), "%H%M"))

# Create a dir for this run.
RunDirectory <- str_c(wd, "/", run_name)
dir.create(RunDirectory)

#Path to FlameAdjust kcp (generalized to be used for all .key scenarios generated here)
treat_kcp_dir <- "D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/OkaWen_kcps_v2_Eireann_feedback"
treat_kcps <- list.files(treat_kcp_dir)
fire_kcps <- "D:/WFSETP/Scenario_Development/Baseline_2020_FVS/simfire_hardcoded.kcp"

#Create a table of FSim fire scenario parameters (flame lengths and associated fuel moisture and weather conditions)
FSim_scenarios <- data.frame(
  flame_length = c(1, 3, 5, 7, 10, 20),
  fm1 = c(8, 7, 6, 5, 4, 3),
  fm10 = c(8, 7, 6, 5, 4, 4),
  fm100 = c(10, 9, 8, 7, 6, 5),
  fm1000 = c(15, 13, 11, 10, 8, 6),
  fmduff = c(50, 45, 40, 30, 20, 15),
  fmlwood = c(110, 100, 90, 80, 60, 50),
  fmlherb = c(110, 100, 90, 70, 40, 40),
  fire_year = rep(4, 6),
  wspd_mph = c(2, 4, 4, 6, 7, 10),
  temp_F = c(85, 85, 85, 90, 90, 90),
  mortality = rep(1, 6),
  per_stand_burned = c(70, 80, 90, 90, 100, 100),
  season = rep(3, 6)
)

#Modification of Rachel's function to generate .key files across FSim scenarios (previously dictated in multiple .kcp files)
createInputFile <- function(stand, managementID, params_row, outputDatabase, treat_kcp, fire_kcp, inputDatabase) {
  paste0(
    'STDIDENT\n', stand, '\n',
    'STANDCN\n', stand, '\n',
    'MGMTID\n', managementID, '\n',
    'NUMCYCLE       3\n',
    'TIMEINT         0        1\n',
    'SCREEN\n',
    'DataBase\n',
    'DSNout\n', outputDatabase, '\n',
    'SUMMARY\n',
    'COMPUTDB\n',
    'CALBSTDB\n',
    
    # ---- Report Database Output ----
    'BURNREDB\n',
    'CARBREDB\n',
    'DWDCVDB\n',
    'FUELREDB\n',
    'FUELSOUT\n',
    'MORTREDB\n',
    'POTFIRDB\n',
    'SNAGOUDB\n',
    'SNAGSUDB\n',
    'STRCLSDB          2\n',
    'CutLiDB           2         2\n',
    'TreeLiDB          2         2\n',
    'End\n',
    
    # ---- Input DB Queries ----
    'DATABASE\n',
    'DSNIN\n', inputDatabase, '\n',
    'StandSQL\n',
    "SELECT * FROM FVS_StandInit\n",
    "WHERE  Stand_ID  = '%stand_cn%'\n",
    'EndSQL\n',
    'DSNIN\n', inputDatabase, '\n',
    'TreeSQL\n',
    'SELECT * FROM FVS_TreeInit\n',
    'WHERE Stand_ID = (SELECT Stand_ID FROM FVS_StandInit\n',
    "WHERE Stand_ID = '%stand_cn%')\n",
    'EndSQL\n',
    'END\n',
    
    # ---- COMPUTE block with scenario inputs ----
    'COMPUTE            0\n',
    'SEV_FL = POTFLEN(1)\n',
    'MOD_FL = POTFLEN(2)\n',
    'scenario = 1\n',
    'CC = acancov\n',
    'FML = fuelmods(1,1)\n',
    'CHT = ATOPHT\n',
    'CBH = crbaseht\n',
    'CBD = crbulkdn\n',
    'YR = year\n',
    'FLEN = ', params_row$flame_length, '\n',
    'FM1 = ', params_row$fm1, '\n',
    'FM10 = ', params_row$fm10, '\n',
    'FM100 = ', params_row$fm100, '\n',
    'FM1000 = ', params_row$fm1000, '\n',
    'FMDUFF = ', params_row$fmduff, '\n',
    'FMLWOOD = ', params_row$fmlwood, '\n',
    'FMLHERB = ', params_row$fmlherb, '\n',
    'FIR_YEAR = ', params_row$fire_year, '\n',
    'WSPD = ', params_row$wspd_mph, '\n',
    'TEMP = ', params_row$temp_F, '\n',
    'FVS_MORT = ', params_row$mortality, '\n',
    'FVSpBURN = ', params_row$per_stand_burned, '\n',
    'FVS_SEAS = ', params_row$season, '\n',
    'END\n',
    
    # ---- Fire Reports ----
    'FMIN\n',
    'BURNREPT\n',
    'CanFProf\n',
    'CarbRept\n',
    'DWDCvOut\n',
    'FUELREPT\n',
    'FUELOUT\n',
    'MORTREPT\n',
    'POTFIRE\n',
    'SNAGOUT\n',
    'SNAGSUM\n',
    'FIRECALC           0         1         2\n',
    'END\n',
    
    # ---- Tree outputs and classification ----
    'TreeList        1\n',
    'TreeList        2\n',
    'CALBSTAT\n',
    'CutList\n',
    'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
    
    # ---- ADDFILE links to shared .kcp ----
    'OPEN              81\n',
    treat_kcp, '\n',
    'ADDFILE           81\n',
    'CLOSE             81\n',
    'OPEN              81\n',
    fire_kcp, '\n',
    'ADDFILE           81\n',
    'CLOSE             81\n',
    
    'Process\n\n'
  )
}

##--------createInputFiles_no_fire
createInputFile_no_trt <- function(stand, managementID, outputDatabase, inputDatabase) {
  paste0(
    'STDIDENT\n', stand, '\n',
    'STANDCN\n', stand, '\n',
    'MGMTID\n', managementID, '\n',
    'NUMCYCLE       3\n',
    'TIMEINT         0        1\n',
    'SCREEN\n',
    'DataBase\n',
    'DSNout\n', outputDatabase, '\n',
    'SUMMARY\n',
    'COMPUTDB\n',
    'CALBSTDB\n',
    
    # ---- Report Database Output ----
    'BURNREDB\n',
    'CARBREDB\n',
    'DWDCVDB\n',
    'FUELREDB\n',
    'FUELSOUT\n',
    'MORTREDB\n',
    'POTFIRDB\n',
    'SNAGOUDB\n',
    'SNAGSUDB\n',
    'STRCLSDB          2\n',
    'CutLiDB           2         2\n',
    'TreeLiDB          2         2\n',
    'End\n',
    
    # ---- Input DB Queries ----
    'DATABASE\n',
    'DSNIN\n', inputDatabase, '\n',
    'StandSQL\n',
    "SELECT * FROM FVS_StandInit\n",
    "WHERE  Stand_ID  = '%stand_cn%'\n",
    'EndSQL\n',
    'DSNIN\n', inputDatabase, '\n',
    'TreeSQL\n',
    'SELECT * FROM FVS_TreeInit\n',
    'WHERE Stand_ID = (SELECT Stand_ID FROM FVS_StandInit\n',
    "WHERE Stand_ID = '%stand_cn%')\n",
    'EndSQL\n',
    'END\n',
    
    # ---- COMPUTE block with scenario inputs ----
    'COMPUTE            0\n',
    'SEV_FL = POTFLEN(1)\n',
    'MOD_FL = POTFLEN(2)\n',
    'scenario = 1\n',
    'CC = acancov\n',
    'FML = fuelmods(1,1)\n',
    'CHT = ATOPHT\n',
    'CBH = crbaseht\n',
    'CBD = crbulkdn\n',
    'YR = year\n',
    'END\n',
    
    # ---- Fire Reports ----
    'FMIN\n',
    'BURNREPT\n',
    'CanFProf\n',
    'CarbRept\n',
    'DWDCvOut\n',
    'FUELREPT\n',
    'FUELOUT\n',
    'MORTREPT\n',
    'POTFIRE\n',
    'SNAGOUT\n',
    'SNAGSUM\n',
    'FIRECALC           0         1         2\n',
    'END\n',
    
    # ---- Tree outputs and classification ----
    'TreeList        1\n',
    'TreeList        2\n',
    'CALBSTAT\n',
    'CutList\n',
    'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
    
    'Process\n\n'
  )
}

write_keywords_parallel <- function(database_path, FSim_scenarios, treat_kcps, fire_kcps) {
  # Load stand data once
  con <- DBI::dbConnect(RSQLite::SQLite(), database_path)
  standinit <- DBI::dbReadTable(con, "FVS_STANDINIT")
  DBI::dbDisconnect(con)
  
  #Create a cross-product of scenarios, treatment kcps, and fire kcps
  run_matrix <- expand_grid(FSim_scenarios,
                            treat_kcp = treat_kcps,
                            fire_kcp = fire_kcps)
  
  #Use future_pmap to parallelize across each row
  plan(multisession, workers = parallel::detectCores() - 1)
  
  # Create scenario-specific .key and .in files in parallel
  future_pmap(run_matrix, function(...){
    row <- list(...)
    
    fl_label <- paste0("FL", row$flame_length)
    
    #Compose a unique scenario ID with flame length, treatment, and fire KCPs
    treatment_name <- tools::file_path_sans_ext(basename(row$treat_kcp))
    fire_name <- tools::file_path_sans_ext(basename(row$fire_kcp))
    scenario_id <- paste0("FL", row$flame_length, "_", treatment_name, "_", fire_name)
    
    key_text_all <- character()
    
    for (j in seq_len(nrow(standinit))) {
      stand <- standinit$Stand_ID[j]
      variant <- standinit$Variant[j]
      key_name <- paste0(scenario_id, "_", variant)
      out_db <- paste0("./outputs/", key_name, ".db")
      
      key_text_all <- c(
        key_text_all,
        createInputFile(
          stand = stand,
          managementID = key_name,
          params_row = row,
          outputDatabase = out_db,
          treat_kcp = treat_kcp,
          fire_kcp = fire_kcp,
          inputDatabase = database_path
        )
      )
    }
    
    # Append STOP
    key_text_all <- c(key_text_all, "STOP\n")
    
    # Write output
    key_file <- file.path(RunDirectory, paste0(scenario_id, ".key"))
    in_file <- file.path(RunDirectory, paste0(scenario_id, ".in"))
    
    writeLines(key_text_all, key_file)
    
    fvs_in <- paste0(
      scenario_id, ".key\n",
      scenario_id, ".fvs\n",
      scenario_id, ".out\n",
      scenario_id, ".trl\n",
      scenario_id, ".sum\n"
    )
    
    writeLines(fvs_in, in_file)
  })
}

write_keywords_no_trt <- function(database_path){
  #Connect to the database and extract the FVS_STANDINIT table
  con <- dbConnect(SQLite(), database_path)
  standinit <- dbReadTable(con, "FVS_STANDINIT")
  dbDisconnect(con)
  
  #Create the stand list - fields should be Stand_ID, Variant, and kcp
  standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                      Variant = standinit$Variant)
  #Create a key label 
  treat_key <- "Scenario_FL0"
  
  #Character vector to store the key file text before appending STOP
  key_text_all <- character()
  
  for(j in seq_len(nrow(standinit))){
    key_text_all <- c(
      #Create keywords for each unique combination of kcp & variant
      #Append to the character vector
      createInputFile_no_trt(stand = standlist$Stand_ID, 
                             managementID = standlist$Stand_ID, 
                             outputDatabase = paste0("./outputs/FL0_EC.db"), 
                             inputDatabase = database_path)
    )
  }
  
  #Append STOP
  key_text_all <- c(key_text_all, "STOP\n")
  
  # Write output
  key_file <- file.path(RunDirectory, paste0(scenario_id, ".key"))
  in_file <- file.path(RunDirectory, paste0(scenario_id, ".in"))
  
  writeLines(key_text_all, key_file)
  
  fvs_in <- paste0(
    scenario_id, ".key\n",
    scenario_id, ".fvs\n",
    scenario_id, ".out\n",
    scenario_id, ".trl\n",
    scenario_id, ".sum\n"
  )
  
  writeLines(fvs_in, in_file)
} #End function to write key and in files


#Write the .key files using the above functions
write_keywords_no_trt(EC_dbs)
#Write the scenario .key files in parallel
write_keywords_parallel(EC_dbs, FSim_scenarios, treat_kcps, fire_kcps)
#Reset parallel backend
plan(sequential)

#Create the data frame of run scenario parameters to parallelize over
#The number of rows is the number of FVS runs you're doing
variants <- "ec"
kcps <- "simfire_hardcoded"
flame_lengths <- c(1, 3, 5, 7, 10, 20)
runs <- expand.grid(variant = variants, kcp = kcps, flame_length = flame_lengths)
runs <- rbind(runs, c("ec", NA, 0))
print(runs)

#Make an outputs directory
dir.create(paste0(RunDirectory, "/outputs"))

runFVS <- function(variant, kcp, flame_length, RunDirectory, fvs_bin) {
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Set the command line (keyword file)
    print(paste0("--keywordfile=Scenario_FL", flame_length, ".key"))
    rFVS::fvsSetCmdLine(
      cl = paste0("--keywordfile=Scenario_FL", flame_length, ".key"),
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
    flame_length = flame_length,
    status = retCode
  )
}

n_runs <- nrow(runs)
future::plan(multisession, workers = n_runs)

#Run FVS in parallel for all combinations
furrr::future_pmap(
  list(
    variant = runs$variant,
    kcp = runs$kcp,
    flame_length=runs$flame_length
  ),
  runFVS,
  RunDirectory = RunDirectory,
  fvs_bin = fvs_bin
)

plan(sequential)
