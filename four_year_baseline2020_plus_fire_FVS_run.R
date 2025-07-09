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

setwd("C:/WFSETP_FVS/Baseline_2020_FVS")
wd <- getwd()

#Set the FVS executable folder
fvs_bin = "C:/FVS/FVSSoftware/FVSbin"

#---Set location of TMFM sqlite databases and read in data----------------------
TMFM2020_dir_path <- "C:/WFSETP_FVS/TMFM_2020_OkaWen_Databases"

okawen_dbs <- list.files(TMFM2020_dir_path,
                         full.names=TRUE)
#We only need to run this for the East Cascades variant
EC_dbs <- okawen_dbs[1]

#Name the FVS run
run_name <- str_c("baseline_2020_with_fire_FVS_run",
                  strftime(Sys.Date(), "%d%b%y"),
                  "_", strftime(Sys.time(), "%H%M"))

# Create a dir for this run.
RunDirectory <- str_c(wd, "/", run_name)
dir.create(RunDirectory)

setwd(paste0(RunDirectory))

#Save the session information to record which R script was used, when, and with which package versions, to do the run
#Generated with ChatGPT
log_session_info <- function(script_name = NULL, log_file = "session_log.txt") {
  # Get date
  current_date <- Sys.Date()
  
  # Get session info (only package names and versions)
  pkg_info <- sessionInfo()$otherPkgs
  pkg_versions <- sapply(pkg_info, function(pkg) paste(pkg$Package, pkg$Version))
  
  # Determine script name
  if (is.null(script_name)) {
    script_name <- tryCatch({
      # Try commandArgs() for command-line execution
      args <- commandArgs(trailingOnly = FALSE)
      script_path <- sub("--file=", "", args[grep("--file=", args)])
      if (length(script_path) > 0) {
        basename(script_path)
      } else if (requireNamespace("rstudioapi", quietly = TRUE) &&
                 rstudioapi::isAvailable()) {
        # If in RStudio, use the active document name
        rstudioapi::getActiveDocumentContext()$path |>
          basename()
      } else {
        "Unknown_Script"
      }
    }, error = function(e) "Unknown_Script")
  }
  
  # Compose the log text
  log_text <- c(
    paste0("Date: ", current_date),
    paste0("Script: ", script_name),
    "Package Versions:",
    pkg_versions,
    ""  # Blank line for spacing
  )
  
  # Write to file (append mode)
  con <- file(log_file, open = "a")  # Open in append mode
  writeLines(log_text, con = con)
  close(con)
}

log_session_info()

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

                            create_fire_kcp_files <- function(params_df, output_dir = "./fire_kcps", base_filename = "FlameAdjust") {
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  for (i in seq_len(nrow(params_df))) {
    p <- params_df[i, ]
    
    # Build the kcp text with hardcoded values
    kcp_text <- paste0(
      "!! Auto-generated fire KCP file based on scenario ", i, "\n",
      "!! Variables hard-coded from parameters\n\n",

      "*Keyword | Field 1 | Field 2 | Field 3 | Field 4 | Field 5 | Field 6 | Field 7 |\n",
      "* -------+---------+---------+---------+---------+---------+---------+---------+\n",

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
fire_kcps <- list.files(paste0(RunDirectory, "/fire_kcps"), full.names = TRUE)

createInputFile_no_trt_fire <- function(stand, managementID, params_row, outputDatabase, areaSpecificKcp, inputDatabase) {
  paste0(
    'STDIDENT\n', stand, '\n',
    'STANDCN\n', stand, '\n',
    'MGMTID\n', managementID, '\n',
    'NUMCYCLE       4\n',
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
    'FIRECALC           3\n',
    'END\n',
    
    # ---- Tree outputs and classification ----
    'TreeList        1\n',
    'TreeList        2\n',
    'TreeList        3\n',
    'CALBSTAT\n',
    'CutList\n',
    'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
    
    # # ---- CYCLE block to apply fire .kcp in year 4 (cycle 3) ----
    'OPEN              81\n',
    fire_kcp, '\n',
    'ADDFILE           81\n',
    'CLOSE             81\n',
    'END\n',
    
    'Process\n\n'
  )
}

write_keywords_parallel <- function(database_path, FSim_scenarios, kcp_path) {
  # Load stand data once
  con <- DBI::dbConnect(RSQLite::SQLite(), database_path)
  standinit <- DBI::dbReadTable(con, "FVS_STANDINIT")
  DBI::dbDisconnect(con)
  
  # Create scenario-specific .key and .in files in parallel
  future_pmap(FSim_scenarios, function(...){
    inputs <- list(...)
    
    fl_label <- paste0("FL", inputs$flame_length)
    scenario_id <- paste0("Scenario_", fl_label)
    
    key_text_all <- character()
    
    for (j in seq_len(nrow(standinit))) {
      stand <- standinit$Stand_ID[j]
      variant <- standinit$Variant[j]
      key_name <- paste0(fl_label, "_", variant)
      out_db <- paste0("./outputs/", key_name, ".db")
      
      key_text_all <- c(
        key_text_all,
        createInputFile_no_trt_fire(
          stand = stand,
          managementID = key_name,
          params_row = inputs,
          outputDatabase = out_db,
          areaSpecificKcp = kcp_path,
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
#plan(multisession, workers = parallel::detectCores() - 1) 
plan(multisession, workers = 12)
write_keywords_parallel(EC_dbs, FSim_scenarios, fire_kcp)
#Reset parallel backend
plan(sequential)

#Create the data frame of run scenario parameters to parallelize over
#The number of rows is the number of FVS runs you're doing
variants <- "ec"
kcps <- "simfire_hardcoded"
flame_lengths <- c(1, 3, 5, 7, 10, 20)
runs <- expand.grid(variant = variants, kcp = kcps, flame_length = flame_lengths)
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

#n_runs <- nrow(runs)
future::plan(multisession, workers = 30)

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

combine_dbs_general <- function(RunDirectory, rm.files = FALSE, output_db_name = "Full_Set_FVSOut.db") {
  #List all the .db files in the directory, excluding the final merged output if it already exists
  list_dbs <- list.files(RunDirectory, pattern = "\\.db$", full.names = TRUE)
  list_dbs <- list_dbs[!str_detect(list_dbs, output_db_name)] #Removes the output DB if it already exists
  
  #Get the union of all table names that exist across all databases
  all_table_names <- unique(unlist(
    lapply(list_dbs, function(fn) {
      con <- dbConnect(SQLite(), fn)
      tbls <- tryCatch(dbListTables(con), error = function(e) character(0)) #If there's an error, return an empty character vector
      dbDisconnect(con)
      tbls
    })
  ))
  
  #Read each table from each DB (if it exists), and tag it with the DB file name
  allframes <- lapply(list_dbs, function(fn) {
    con <- dbConnect(SQLite(), fn)
    db_label <- basename(fn) #get just the filename
    
    #For each table name, try to read it from the current database
    tbls <- lapply(all_table_names, function(tbl) {
      df <- tryCatch(dbReadTable(con, tbl), error = function(e) NULL) #Read the table or return NULL
      if (!is.null(df) && nrow(df) > 0) {
        df$db <- db_label  #Add a new column with the source database label to track which entries came from which database
      }
      if (!is.null(df) && nrow(df) == 0) {
        message("Note: Table '", tbl, "' in '", db_label, "' is empty.")
      }
      
      df
    })
    names(tbls) <- all_table_names #Name the list entries by table name
    dbDisconnect(con)
    tbls
  })
  
  #For each table name, combine all the available versions from all databases
  combined_tables <- list()
  for (tbl in all_table_names) {
    tbl_list <- lapply(allframes, function(x) x[[tbl]]) #Get that table from each database
    tbl_list <- tbl_list[!sapply(tbl_list, is.null)] #Filter out any that were missing (NULL)
    if (length(tbl_list) > 0) {
      combined_tables[[tbl]] <- rbindlist(tbl_list, fill = TRUE) #Combine all, filling in NAs for missing columns
    }
  }
  
  #Write each combined table to a new SQLite database
  con_out <- dbConnect(SQLite(), file.path(RunDirectory, output_db_name))
  for (tbl in names(combined_tables)) {
    df <- combined_tables[[tbl]]
    if (nrow(df) > 0) {
      dbWriteTable(con_out, tbl, df) #Create a new table in the output database
    }
  }
  dbDisconnect(con_out)
  
  #Optional: clean up source files
  #Chris had this in his function, but I made the default argument false in case you want to keep the OG databases.
  #This does save space and saves a few seconds of highlighting the original database files to delete.
  if (rm.files) {
    file.remove(list_dbs)
  }
  
  #Memory cleanup
  rm(allframes, combined_tables)
  invisible(gc())
}

database_dir <- paste0(RunDirectory, "/outputs/")

combine_dbs_general(RunDirectory = database_dir, rm.files = FALSE, output_db_name = "Combined_Outputs_All_FLs_txs.db")
