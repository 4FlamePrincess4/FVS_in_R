library(tidyverse) #For data manipulation
library(RSQLite) #For working with sqlite databases
library(DBI) #For interacting with different databases in R
library(terra) #For raster and vector geospatial operations
library(data.table) #Faster data format, works with default R dataframe functions
library(foreign) #For reading in .dbf files
library(furrr) #For parallelization
library(withr) #For parallelization - used to temporarily change the working directory for each worker

#Install the rFVS package
install.packages("remotes")
remotes::install_github("SilviaTerra/rFVS")
library(rFVS)

setwd("D:/WFSETP/Scenario_Development/Treatment_Runs_FVS")
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
run_name <- str_c("treat_plus_fire_",
                  strftime(Sys.Date(), "%d%b%y"),
                  "_", strftime(Sys.time(), "%H%M"))

# Create a dir for this run.
RunDirectory <- str_c(wd, "/", run_name)
dir.create(RunDirectory)

setwd(paste0(RunDirectory))

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
  moisture = rep("",6),
  temp_F = c(85, 85, 85, 90, 90, 90),
  mortality = rep(1, 6),
  per_stand_burned = c(70, 80, 90, 90, 100, 100),
  season = rep(3, 6)
)

create_fire_kcp_files <- function(params_df, output_dir = "./fire_kcps", base_filename = "FlameAdjust") {
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  for (i in seq_len(nrow(params_df))) {
    p <- params_df[i, ]
    
    # Build the kcp text with hardcoded values
    kcp_text <- paste0(
      "!! Auto-generated fire KCP file based on scenario ", i, "\n",
      "!! Variables hard-coded from parameters\n\n",
      
      "COMPUTE           0\n",
      "FLEN = ", p$flame_length, "\n",
      "CPC = 0  \n",
      "PCHt2Lv = TmpHt2Lv\n",
      "CBH=CRBASEHT\n",
      "TmpHt2Lv = CBH\n",
      "PCCBD=TmpCBD\n",
      "CBD=CRBULKDN\n",
      "TmpCBD=CBD\n",
      "HSM = 6.026 * (FLEN / 3.2808) ** 1.4466\n",
      "HS_ = HSM * 3.2808\n",
      "END\n\n",
      
      "IF                0\n",
      "PCCBD GT 0.0001\n",
      "THEN\n",
      "COMPUTE           0\n",
      "Io_ = (0.010 * PCHt2Lv * 0.3048 * (460.0 + 25.9 * 100)) ** (3 / 2)\n",
      "FLCRIT = (0.07749 * Io_ ** 0.46) * 3.281\n",
      "FLCRIT2 = 0.3 * BTOPHT\n",
      "FSLOPE = 0.9 / (FLCRIT2 - FLCRIT)\n",
      "NTERCEPT = 1.0 - (FSLOPE * FLCRIT2)\n",
      "Y_ = MAX((FLEN * FSLOPE + NTERCEPT), 0.0)\n",
      "YSQRT = SQRT(Y_)\n",
      "fplace = maxindex(FLEN, FLCRIT)\n",
      "CPC = (INDEX(fplace, MIN(YSQRT, 1), 0)) * 100\n",
      "Done = 1\n",
      "END\n",
      "ENDIF\n\n",
      
      "FMIN\n",
      "!! Fuel moisture by size class\n",
      "!!moisture     year       1hr      10hr     100hr    1000hr      duff   lvwoody    lvherb\n",
      "MOISTURE          ", p$fire_year, "         ",
      p$fm1, "         ", p$fm10, "       ", p$fm100, "       ",
      p$fm1000, "        ", p$fmduff, "      ", p$fmlwood, "      ", p$fmlherb, "\n",
      "!! Flame adjustment inputs for flame length scenario\n",
      "FLAMEADJ          ", p$fire_year, "     PARMS(1.0, ", p$flame_length, ", CPC, HS_)\n",
      "!!simfire      year   wind(mph)  moisture    temp(F)  mortality  %burned   season\n",
      "SIMFIRE           ", p$fire_year, "         ",
      p$wspd_mph, "         ", p$moisture, "         ", p$temp_F, "        ", p$mortality, "       ", p$per_stand_burned, "         ", p$season, "\n",
      "END\n"
    )
    
    # Write to file
    kcp_filename <- file.path(output_dir, paste0(base_filename, "_", p$flame_length, ".kcp"))
    writeLines(kcp_text, con = kcp_filename)
  }
}

create_fire_kcp_files(FSim_scenarios)

#Paths to kcps
treat_kcps <- list.files(paste0(RunDirectory, "/treat_kcps"), full.names = TRUE)
fire_kcps <- list.files(paste0(RunDirectory, "/fire_kcps"), full.names = TRUE)

#Modification of Rachel's function to generate .key files across FSim scenarios (previously dictated in multiple .kcp files)
createInputFile <- function(stand, managementID, params_row, outputDatabase, treat_kcp, fire_kcp, inputDatabase) {
  paste0(
    'STDIDENT\n', stand, '\n',
    'STANDCN\n', stand, '\n',
    'MGMTID\n', managementID, '\n',
    'NUMCYCLE        4\n',
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
    'CutLiDB           2         2         2\n',
    'TreeLiDB          2         2         2\n',
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
    'FIRECALC           3\n',
    'END\n',
    
    # ---- Tree outputs and classification ----
    'TreeList        1\n',
    'TreeList        2\n',
    'TreeList        3\n',
    'CALBSTAT\n',
    'CutList\n',
    'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
    
    # ---- ADDFILE links to shared .kcp ----
    'OPEN              81\n',
    treat_kcp, '\n',
    'ADDFILE           81\n',
    'CLOSE             81\n',
    
    # ---- CYCLE block to apply fire .kcp in year 4 (cycle 3) ----
    'OPEN              81\n',
    fire_kcp, '\n',
    'ADDFILE           81\n',
    'CLOSE             81\n',
    'END\n',
    
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
  plan(multisession, workers = 20)
  
  # Create scenario-specific .key and .in files in parallel
  future_pmap(run_matrix, function(...){
    inputs <- list(...)
    
    fl_label <- paste0("FL", inputs$flame_length)
    
    #Compose a unique scenario ID with flame length, treatment, and fire KCPs
    treatment_name <- tools::file_path_sans_ext(basename(inputs$treat_kcp))
    fire_name <- tools::file_path_sans_ext(basename(inputs$fire_kcp))
    scenario_id <- paste0("FL", inputs$flame_length, "_", treatment_name, "_", fire_name)
    
    message("Running for FL = ", inputs$flame_length, 
            ", treatment_kcp = ", inputs$treat_kcp, 
            ", fire_kcp = ", inputs$fire_kcp)
    
    key_text_all <- character()
    
    #Subset just the parameters from the inputs
    params_row <- inputs[c(
      "flame_length", "fm1", "fm10", "fm100", "fm1000",
      "fmduff", "fmlwood", "fmlherb", "wspd_mph", 
      "temp_F", "mortality", "per_stand_burned", "season"
    )]
    
    
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
          params_row = params_row,
          outputDatabase = out_db,
          treat_kcp = inputs$treat_kcp,
          fire_kcp = inputs$fire_kcp,
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

#Write the scenario .key files in parallel
write_keywords_parallel(EC_dbs, FSim_scenarios, treat_kcps, fire_kcps)
#Reset parallel backend
plan(sequential)

#Create the data frame of run scenario parameters to parallelize over
#The number of rows is the number of FVS runs you're doing
variants <- "ec"
treatments <- list.files("./treat_kcps")
treatments <- stringr::str_sub(treatments, end = -5) 
fire_scenarios <- list.files("./fire_kcps")
fire_scenarios <- stringr::str_sub(fire_scenarios, end = -5) 
flame_lengths <- c(1, 3, 5, 7, 10, 20)
runs <- expand.grid(variant=variants, flame_length = flame_lengths, treatment = treatments, fire_kcp = fire_scenarios,
                    stringsAsFactors = FALSE)
print(runs)

#Make an outputs directory
dir.create(paste0(RunDirectory, "/outputs"))

runFVS <- function(variant, flame_length, treatment, fire_kcp, RunDirectory, fvs_bin) {
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Set the command line (keyword file)
    print(paste0("--keywordfile=FL",flame_length, "_", treatment, "_", fire_kcp, ".key"))
    rFVS::fvsSetCmdLine(
      cl = paste0("--keywordfile=FL",flame_length, "_", treatment, "_", fire_kcp, ".key"),
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
    treatment = treatment,
    status = retCode
  )
}

n_runs <- nrow(runs)
future::plan(multisession, workers = 25) #there are 30 runs so I'm going to set this lower to avoid crashing my computer

#Run FVS in parallel for all combinations
furrr::future_pmap(
  list(
    variant = runs$variant,
    flame_length=runs$flame_length,
    treatment=runs$treatment,
    fire_kcp=runs$fire_kcp
  ),
  runFVS,
  RunDirectory = RunDirectory,
  fvs_bin = fvs_bin
)

plan(sequential)