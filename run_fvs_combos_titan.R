# Load libraries
library(stringr)
library(dplyr)
library(purrr)
library(furrr)
library(rFVS)
library(RSQLite)

# ---- USER INPUTS ----
# case_lists      <- case_lists            # Your case list from earlier step
kcp_dir         <- "C:/WFSETP/Test_Scenario/treat_kcps"   # Directory with all treatment KCPs
fvs_bin         <- "C:/FVS/FVSSoftware/FVSbin"    # Path to FVS executable
n_cores         <- 32                     # Number of parallel workers
wd              <- "C:/WFSETP/Scenario1/FVS_runs_20th_okawen"  # Specify the working directory
input_dbs        <- "C:/WFSETP/Data/TMFM_2020_OkaWen_Databases"

setwd(wd)

#Read in the standid_list from the last script
standid_file <- "C:/WFSETP/Scenario1/standid_list_20th.rds"
standid_list <- readRDS(standid_file)

#Remove any cases where there are only NA values for StandIDs
removed_combos <- names(standid_list[sapply(standid_list, function(x) all(is.na(x)))])
print(paste0("These combinations had only NA values for stand IDs and were skipped: ", removed_combos))
standid_list <- standid_list[!sapply(standid_list, function(x) all(is.na(x)))]

# ---- HELPER FUNCTIONS ----

# Get KCP file path for treatment
get_kcp_path <- function(trt_code) {
  if (is.na(trt_code) || trt_code == "NA") return(NA)
  kcp_file <- list.files(kcp_dir, pattern = paste0(trt_code, "\\.kcp$"), full.names = TRUE)
  if (length(kcp_file) == 0) stop(paste("No KCP found for treatment:", trt_code))
  kcp_file[1]
}

no_yr1_fire_if_rx_fire <- function(combo_name) {
  # Check if Trt1000 exists AND Year 1 fire exists
  has_trt900 <- grepl("Trt900", combo_name)
  has_fire_y1 <- grepl("Y1:[0-9]+_F:[0-9]+", combo_name)
  
  if (has_trt900 && has_fire_y1) {
    # Replace Y1:1_F:1 (or any Y1 fire) with Y1:NA_F:NA
    combo_name <- sub("Y1:[0-9]+_F:[0-9]+", "Y1:NA_F:NA", combo_name)
  }
  
  return(combo_name)
}


# Classify case based on treatment & fire
classify_case <- function(treatment, fire_events) {
  # Ensure values are not NULL
  treatment <- ifelse(is.null(treatment), NA, treatment)
  fire_events <- ifelse(is.null(fire_events), NA, fire_events)
  
  # Logical checks safely return TRUE/FALSE
  has_trt <- !is.na(treatment) && treatment != "NA"
  has_fire <- !is.na(fire_events) && fire_events != "None" && grepl("\\d", fire_events)
  
  # Force to logical TRUE/FALSE
  has_trt <- isTRUE(has_trt)
  has_fire <- isTRUE(has_fire)
  
  # Classify
  if (!has_trt && !has_fire) return("no_fire_no_trt")
  if (has_trt && !has_fire)  return("trt_no_fire")
  if (has_trt && has_fire)   return("trt_with_fire")
  if (!has_trt && has_fire)  return("fire_no_trt")
}

parse_fire_events <- function(combo_name) {
  matches <- regmatches(combo_name, gregexpr("Y(\\d+):([0-9NA]+)_F:([0-9NA]+)", combo_name))
  print(paste0("This is the combo name before parsing:", combo_name))
  print(paste0("These are the combo name matches after parsing:", paste0(matches[[1]], collapse=", ")))
  
  fire_events <- lapply(matches[[1]], function(ev) {
    parts <- unlist(strsplit(ev, "_F:"))
    year_part <- sub("Y([0-9]+):[0-9NA]+", "\\1", parts[1])
    flame_length <- parts[2]  # take the number after "_F:"
    
    if (flame_length != "NA") {
      return(data.frame(
        fire_year = as.numeric(year_part),
        flame_length = as.numeric(flame_length)
      ))
    } else {
      return(NULL)
    }
  })
  
  do.call(rbind, fire_events)
}



build_params_df <- function(combo_name, FSim_scenarios) {
  fire_events <- parse_fire_events(combo_name)
  
  if (is.null(fire_events) || nrow(fire_events) == 0) return(NULL)
  
  # Ensure both sides are numeric for flame_length
  fire_events$flame_length <- as.numeric(fire_events$flame_length)
  FSim_scenarios$flame_length <- as.numeric(FSim_scenarios$flame_length)
  
  # Join with FSim_scenarios by flame_length
  params_df <- dplyr::left_join(fire_events, FSim_scenarios, by = "flame_length")
  
  # Overwrite fire_year column with actual year from combo_name
  params_df$fire_year <- fire_events$fire_year
  
  # Set fixed values for moisture, mortality, season if needed
  params_df$moisture <- ""
  params_df$mortality <- 1
  params_df$season <- 3
  
  return(params_df)
}

safe_name <- function(name) {
  name |>
    (\(x) gsub(":", "_", x))() |>    # Replace colons with underscores
    (\(x) gsub("[[:space:]]+", "_", x))()  # Replace spaces with underscores if any
}

#Function to combine databases in the outputs folder
combine_dbs_recursive <- function(RunDirectory, rm.files = FALSE, output_db_name = "Full_Set_FVSOut.db") {
  library(DBI)
  library(RSQLite)
  library(data.table)
  library(stringr)
  
  # List all .db files in all subdirectories under "outputs"
  list_dbs <- list.files(
    path = RunDirectory, 
    pattern = "\\.db$", 
    full.names = TRUE, 
    recursive = TRUE
  )

  if (length(list_dbs) == 0) {
    stop("No database files found in the outputs directories.")
  }
  
  # Get the union of all table names across all databases
  all_table_names <- unique(unlist(
    lapply(list_dbs, function(fn) {
      con <- dbConnect(SQLite(), fn)
      tbls <- tryCatch(dbListTables(con), error = function(e) character(0))
      dbDisconnect(con)
      tbls
    })
  ))
  
  # Read tables from each DB and tag with DB file name
  allframes <- lapply(list_dbs, function(fn) {
    con <- dbConnect(SQLite(), fn)
    db_label <- basename(fn)
    
    tbls <- lapply(all_table_names, function(tbl) {
      df <- tryCatch(dbReadTable(con, tbl), error = function(e) NULL)
      if (!is.null(df) && nrow(df) > 0) df$db <- db_label
      if (!is.null(df) && nrow(df) == 0) message("Note: Table '", tbl, "' in '", db_label, "' is empty.")
      df
    })
    names(tbls) <- all_table_names
    dbDisconnect(con)
    tbls
  })
  
  # Combine tables across all databases
  combined_tables <- list()
  for (tbl in all_table_names) {
    tbl_list <- lapply(allframes, function(x) x[[tbl]])
    tbl_list <- tbl_list[!sapply(tbl_list, is.null)]
    if (length(tbl_list) > 0) {
      combined_tables[[tbl]] <- rbindlist(tbl_list, fill = TRUE)
    }
  }
  
  # Write combined tables to new SQLite database
  con_out <- dbConnect(SQLite(), file.path(RunDirectory, output_db_name))
  for (tbl in names(combined_tables)) {
    df <- combined_tables[[tbl]]
    if (nrow(df) > 0) dbWriteTable(con_out, tbl, df)
  }
  dbDisconnect(con_out)
  
  # Optional: remove original database files
  if (rm.files) file.remove(list_dbs)
  
  # Memory cleanup
  rm(allframes, combined_tables)
  invisible(gc())
}


#Function to log the version information for all packages
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

# --- Functions for each workflow ---
run_no_fire_no_trt <- function(combo_name, variant, stands, kcp_dir) {
  message("Running no_fire_no_trt for variant ", variant)
  
  setwd(wd)
  
  okawen_dbs <- list.files(input_dbs,
                           full.names=TRUE)
  names(okawen_dbs) <- toupper(gsub("^.*_([A-Z]{2})\\.db$", "\\1", basename(okawen_dbs)))
  input_db <- okawen_dbs[[variant]]
  
  combo_name_safe <- safe_name(combo_name)
  
  #Name the FVS run
  run_name <- str_c(paste0(combo_name_safe),
                    strftime(Sys.Date(), "%d%b%y"),
                    "_", strftime(Sys.time(), "%H%M"))
  
  # Create a dir for this run.
  RunDirectory <- str_c(wd, "/", run_name)
  dir.create(RunDirectory)
  
  stand_log_file <- file.path(RunDirectory, "stands.txt")
  writeLines(paste0(stands), con = stand_log_file)
  
  createInputFile_no_trt <- function(stand, managementID, outputDatabase, inputDatabase) {
    paste0(
      'STDIDENT\n', stand, '\n',
      'STANDCN\n', stand, '\n',
      'MGMTID\n', managementID, '\n',
      'NUMCYCLE       5\n',
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
      # 'TreeList        1\n',
      # 'TreeList        2\n',
      # 'TreeList        3\n',
      'CALBSTAT\n',
      'CutList\n',
      'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
      
      'Process\n\n'
    )
  }
  
  #Make an outputs directory
  dir.create(paste0(RunDirectory, "/outputs"))
  
  outputDatabase <- paste0("./outputs/", run_name, ".db")
  
  #Connect to the database and extract the FVS_STANDINIT table
  con <- dbConnect(SQLite(), input_db)
  standinit <- dbReadTable(con, "FVS_STANDINIT")
  dbDisconnect(con)
  
  #Create the stand list - fields should be Stand_ID and Variant
  standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                      Variant = standinit$Variant,
                                      stringsAsFactors = FALSE)
  stands_to_run <- standlist %>% dplyr::filter(Stand_ID %in% stands)
  if (nrow(stands_to_run) == 0) {
    message("No matching Stand_IDs found in DB for combo ", combo_name, ". Skipping.")
    return(invisible(NULL))
  }
  
  #Character vector to store the key file text before appending STOP
  key_text_all <- character()
  
  for(j in seq_len(nrow(stands_to_run))){
    st <- stands_to_run$Stand_ID[j]
    mgmtID <- paste0(combo_name_safe, "_", st)
    key_text_all <- c(key_text_all,
                      createInputFile_no_trt(stand = st, managementID = mgmtID,
                                             outputDatabase = outputDatabase, inputDatabase = input_db))
  }
  
  #Append STOP
  key_text_all <- c(key_text_all, "STOP\n")
  
  # Write output
  key_file <- file.path(RunDirectory, paste0(combo_name_safe, ".key"))
  in_file <- file.path(RunDirectory, paste0(combo_name_safe, ".in"))
  
  writeLines(key_text_all, con = key_file)
  
  fvs_in <- paste0(
    combo_name_safe, ".key\n",
    combo_name_safe, ".fvs\n",
    combo_name_safe, ".out\n",
    combo_name_safe, ".trl\n",
    combo_name_safe, ".sum\n"
  )
  
  writeLines(fvs_in, con = in_file)
  
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Build and set the keyword file command
    keyword_file <- paste0("--keywordfile=", combo_name_safe, ".key")
    print(keyword_file)
    
    rFVS::fvsSetCmdLine(
      cl = keyword_file,
      PACKAGE = paste0("FVS", variant)
    )
    
    # Run until done
    retCode <- 0
    while (retCode == 0) {
      retCode <- rFVS::fvsRun(PACKAGE = paste0("FVS", variant))
    }
  })
  
  list(variant = variant, status = retCode)
  
  message("Finished combo ", combo_name, " (variant=", variant, "). Output DB: ", outputDatabase)
  return(invisible(list(combo = combo_name, run_dir = RunDirectory, out_db = outputDatabase)))
}

run_trt_no_fire <- function(combo_name, variant, treatment, stands, get_kcp_path) {
  message("Running trt_no_fire for variant ", variant, " with Trt ", treatment)
  
  setwd(wd)
  
  okawen_dbs <- list.files(input_dbs,
                           full.names=TRUE)
  names(okawen_dbs) <- toupper(gsub("^.*_([A-Z]{2})\\.db$", "\\1", basename(okawen_dbs)))
  input_db <- okawen_dbs[[variant]]
  
  combo_name_safe <- safe_name(combo_name)
  
  #Name the FVS run
  run_name <- str_c(paste0(combo_name_safe),
                    strftime(Sys.Date(), "%d%b%y"),
                    "_", strftime(Sys.time(), "%H%M"))
  
  # Create a dir for this run.
  RunDirectory <- str_c(wd, "/", run_name)
  dir.create(RunDirectory)
  
  stand_log_file <- file.path(RunDirectory, "stands.txt")
  writeLines(paste0(stands), con = stand_log_file)
  
  treat_kcps <- list.files(paste0(kcp_dir),
                           full.names=TRUE)
  kcp_file <- treat_kcps[grepl(treatment, treat_kcps)]
  
  createInputFile <- function(stand, managementID, outputDatabase, kcp_file, inputDatabase) {
    paste0(
      'STDIDENT\n', stand, '\n',
      'STANDCN\n', stand, '\n',
      'MGMTID\n', managementID, '\n',
      'NUMCYCLE        5\n',
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
      'FIRECALC           0         1         2\n',
      'END\n',
      
      # ---- Tree outputs and classification ----
      # 'TreeList        1\n',
      # 'TreeList        2\n',
      # 'TreeList        3\n',
      'CALBSTAT\n',
      'CutList\n',
      'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
      
      # ---- ADDFILE links to shared .kcp ----
      'OPEN              81\n',
      kcp_file, '\n',
      'ADDFILE           81\n',
      'CLOSE             81\n',
      
      'Process\n\n'
    )
  }

  #Make an outputs directory
  dir.create(paste0(RunDirectory, "/outputs"))
  
  outputDatabase <- paste0("./outputs/", run_name, ".db")
  
  #Connect to the database and extract the FVS_STANDINIT table
  con <- dbConnect(SQLite(), input_db)
  standinit <- dbReadTable(con, "FVS_STANDINIT")
  dbDisconnect(con)
  
  #Create the stand list - fields should be Stand_ID and Variant
  standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                      Variant = standinit$Variant,
                                      stringsAsFactors = FALSE)
  stands_to_run <- standlist %>% dplyr::filter(Stand_ID %in% stands)
  if (nrow(stands_to_run) == 0) {
    message("No matching Stand_IDs found in DB for combo ", combo_name, ". Skipping.")
    return(invisible(NULL))
  }
  
  kcp_name <- stringr::str_sub(kcp_file, end = -5) 
  
  key_text_all <- character()
  
  for(j in seq_len(nrow(stands_to_run))){
    st <- stands_to_run$Stand_ID[j]
    mgmtID <- paste0(combo_name_safe, "_", st)
    key_text_all <- c(key_text_all,
                      createInputFile(stand = st, 
                                      managementID = mgmtID,
                                      outputDatabase = outputDatabase, 
                                      kcp_file = kcp_file,
                                      inputDatabase = input_db))
  }
  
  #Append STOP
  key_text_all <- c(key_text_all, "STOP\n")
  
  # Write output
  key_file <- file.path(RunDirectory, paste0(combo_name_safe, ".key"))
  in_file <- file.path(RunDirectory, paste0(combo_name_safe, ".in"))
  
  writeLines(key_text_all, con = key_file)
  
  fvs_in <- paste0(
    combo_name_safe, ".key\n",
    combo_name_safe, ".fvs\n",
    combo_name_safe, ".out\n",
    combo_name_safe, ".trl\n",
    combo_name_safe, ".sum\n"
  )
  
  writeLines(fvs_in, con = in_file)
  
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Build and set the keyword file command
    keyword_file <- paste0("--keywordfile=", combo_name_safe, ".key")
    print(keyword_file)
    
    rFVS::fvsSetCmdLine(
      cl = keyword_file,
      PACKAGE = paste0("FVS", variant)
    )
    
    # Run until done
    retCode <- 0
    while (retCode == 0) {
      retCode <- rFVS::fvsRun(PACKAGE = paste0("FVS", variant))
    }
  })
  
  list(variant = variant, status = retCode)
  
  message("Finished combo ", combo_name, " (variant=", variant, "). Output DB: ", outputDatabase)
  return(invisible(list(combo = combo_name, run_dir = RunDirectory, out_db = outputDatabase)))
  
}

run_trt_with_fire <- function(combo_name, variant, treatment, fire_events, stands, get_kcp_path,
                              parse_fire_events, build_params_df, make_fire_filenames) {
  message("Running trt_with_fire for variant ", variant, " with Trt ", treatment, " and fires ", paste(fire_events, collapse = ", "))
  
  setwd(wd)
  
  okawen_dbs <- list.files(input_dbs,
                           full.names=TRUE)
  names(okawen_dbs) <- toupper(gsub("^.*_([A-Z]{2})\\.db$", "\\1", basename(okawen_dbs)))
  input_db <- okawen_dbs[[variant]]
  
  combo_name_safe <- safe_name(combo_name)
  
  #Name the FVS run
  run_name <- str_c(paste0(combo_name_safe),
                    strftime(Sys.Date(), "%d%b%y"),
                    "_", strftime(Sys.time(), "%H%M"))
  
  # Create a dir for this run.
  RunDirectory <- str_c(wd, "/", run_name)
  dir.create(RunDirectory)
  
  stand_log_file <- file.path(RunDirectory, "stands.txt")
  writeLines(paste0(stands), con = stand_log_file)
  
  treat_kcps <- list.files(paste0(kcp_dir),
                           full.names = TRUE)
  kcp_file <- treat_kcps[grepl(treatment, treat_kcps)]
  
  #Create the fire parameters dataframe
  FSim_scenarios <- data.frame(
    flame_length = c(1, 3, 5, 7, 10, 20),
    fm1 = c(8, 7, 6, 5, 4, 3),
    fm10 = c(8, 7, 6, 5, 4, 4),
    fm100 = c(10, 9, 8, 7, 6, 5),
    fm1000 = c(15, 13, 11, 10, 8, 6),
    fmduff = c(50, 45, 40, 30, 20, 15),
    fmlwood = c(110, 100, 90, 80, 60, 50),
    fmlherb = c(110, 100, 90, 70, 40, 40),
    wspd_mph = c(2, 4, 4, 6, 7, 10),
    moisture = rep("",6),
    temp_F = c(85, 85, 85, 90, 90, 90),
    mortality = rep(1, 6),
    per_stand_burned = c(70, 80, 90, 90, 100, 100),
    season = rep(3, 6)
  )
  
  params_df <- build_params_df(combo_name, FSim_scenarios)
  if (is.null(params_df)) {
    message("No fire events for ", combo_name)
    return(NULL)
  }
  print(params_df)
  
    fire_kcp_dir <- paste0(RunDirectory, "/fire_kcps_", combo_name_safe)
  
  create_fire_kcp_files <- function(params_df, fire_kcp_dir, compute = TRUE) {
    dir.create(fire_kcp_dir)
    
    # Generate filenames for only the rows in this subset
    fire_kcp_names <- paste0("fire_yr_", params_df$fire_year, "_fl_", params_df$flame_length, ".kcp")
    
    for (i in seq_len(nrow(params_df))) {
      p <- params_df[i, ]
      
      # Output file path
      kcp_filename <- file.path(fire_kcp_dir, fire_kcp_names[i])
      
      # Build the base kcp text with hardcoded values
      kcp_text <- paste0(
        "!! Auto-generated fire KCP file based on scenario ", i, "\n",
        "!! Variables hard-coded from parameters\n\n",
        
        "*Keyword | Field 1 | Field 2 | Field 3 | Field 4 | Field 5 | Field 6 | Field 7 |\n",
        "* -------+---------+---------+---------+---------+---------+---------+---------+\n")
      
      # Add compute statement text only if applicable
      if(compute) {
        kcp_text <- c(kcp_text, 
                      paste0(
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
                        "ENDIF\n\n"
                        
                      ))
      }
      # Add the FMIN block for all KCPs
      kcp_text <- paste0(kcp_text,
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
      writeLines(kcp_text, con = kcp_filename)
    }
  }
  
  if (nrow(params_df) > 1) {
    message("Multiple fires detected. Using multi-fire KCP logic for later fires.")
    
    # First fire uses full compute version
    create_fire_kcp_files(params_df[1, , drop = FALSE], fire_kcp_dir, compute = TRUE)
    
    # Fires 2–5 use reduced compute version
    create_fire_kcp_files(params_df[-1, , drop = FALSE], fire_kcp_dir, compute = FALSE)
    
  } else {
    # Single fire only → full version
    create_fire_kcp_files(params_df, fire_kcp_dir, compute = TRUE)
  }
  
  #Paths to fire kcps
  fire_kcps <- list.files(paste0(fire_kcp_dir), full.names = TRUE)
  
  createInputFile <- function(stand, managementID, outputDatabase, kcp_file, fire_kcps, inputDatabase) {
    # Helper to create ADDFILE block for any kcp file
    addfile_block <- function(kcp_path) {
      paste0(
        "OPEN              81\n",
        kcp_path, "\n",
        "ADDFILE           81\n",
        "CLOSE             81\n"
      )
    }
    
    # Treatment kcp (always included)
    treatment_block <- addfile_block(kcp_file)
    
    # Fire kcps (loop over vector)
    fire_blocks <- paste(vapply(fire_kcps, addfile_block, FUN.VALUE = character(1)), collapse = "")
    
    
    paste0(
      'STDIDENT\n', stand, '\n',
      'STANDCN\n', stand, '\n',
      'MGMTID\n', managementID, '\n',
      'NUMCYCLE        5\n',
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
      'FIRECALC           0         1         2\n',
      'END\n',
      
      # ---- Tree outputs and classification ----
      # 'TreeList        1\n',
      # 'TreeList        2\n',
      # 'TreeList        3\n',
      'CALBSTAT\n',
      'CutList\n',
      'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
      
      # ---- ADDFILE links to shared .kcp ----
      treatment_block,
      fire_blocks,
      
      'Process\n\n'
    )
  }
  
  #Make an outputs directory
  dir.create(paste0(RunDirectory, "/outputs"))
  
  outputDatabase <- paste0("./outputs/", run_name, ".db")
  
  #Connect to the database and extract the FVS_STANDINIT table
  con <- dbConnect(SQLite(), input_db)
  standinit <- dbReadTable(con, "FVS_STANDINIT")
  dbDisconnect(con)
  
  #Create the stand list - fields should be Stand_ID and Variant
  standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                      Variant = standinit$Variant,
                                      stringsAsFactors = FALSE)
  stands_to_run <- standlist %>% dplyr::filter(Stand_ID %in% stands)
  if (nrow(stands_to_run) == 0) {
    message("No matching Stand_IDs found in DB for combo ", combo_name, ". Skipping.")
    return(invisible(NULL))
  } 
  
  key_text_all <- character()
  
  for(j in seq_len(nrow(stands_to_run))){
    st <- stands_to_run$Stand_ID[j]
    mgmtID <- paste0(combo_name_safe, "_", st)
    key_text_all <- c(key_text_all,
                      createInputFile(stand = st, 
                                      managementID = mgmtID,
                                      outputDatabase = outputDatabase, 
                                      kcp_file = kcp_file,
                                      fire_kcps = fire_kcps,
                                      inputDatabase = input_db))
    
    
    #Append STOP
    key_text_all <- c(key_text_all, "STOP\n")
    
    # Write output
    key_file <- file.path(RunDirectory, paste0(combo_name_safe, ".key"))
    in_file <- file.path(RunDirectory, paste0(combo_name_safe, ".in"))
    
    writeLines(key_text_all, con = key_file)
    
    fvs_in <- paste0(
      combo_name_safe, ".key\n",
      combo_name_safe, ".fvs\n",
      combo_name_safe, ".out\n",
      combo_name_safe, ".trl\n",
      combo_name_safe, ".sum\n"
    )
    
    writeLines(fvs_in, con = in_file)
  }
  
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Build and set the keyword file command
    keyword_file <- paste0("--keywordfile=", combo_name_safe, ".key")
    print(keyword_file)
    
    rFVS::fvsSetCmdLine(
      cl = keyword_file,
      PACKAGE = paste0("FVS", variant)
    )
    
    # Run until done
    retCode <- 0
    while (retCode == 0) {
      retCode <- rFVS::fvsRun(PACKAGE = paste0("FVS", variant))
    }
  })
  
  list(variant = variant, status = retCode)
  
  message("Finished combo ", combo_name, " (variant=", variant, "). Output DB: ", outputDatabase)
  return(invisible(list(combo = combo_name, run_dir = RunDirectory, out_db = outputDatabase)))
  
}

run_fire_no_trt <- function(combo_name, variant, fire_events, stands, 
                            parse_fire_events, build_params_df, make_fire_filenames) {
  message("Running fire_no_trt for variant ", variant, " with fires ", paste(fire_events, collapse = ", "))
  
  setwd(wd)
  
  okawen_dbs <- list.files(input_dbs,
                           full.names=TRUE)
  names(okawen_dbs) <- toupper(gsub("^.*_([A-Z]{2})\\.db$", "\\1", basename(okawen_dbs)))
  input_db <- okawen_dbs[[variant]]
  
  combo_name_safe <- safe_name(combo_name)
  
  #Name the FVS run
  run_name <- str_c(paste0(combo_name_safe),
                    strftime(Sys.Date(), "%d%b%y"),
                    "_", strftime(Sys.time(), "%H%M"))
  
  # Create a dir for this run.
  RunDirectory <- str_c(wd, "/", run_name)
  dir.create(RunDirectory)
  
  stand_log_file <- file.path(RunDirectory, "stands.txt")
  writeLines(paste0(stands), con = stand_log_file)
  
  #Create the fire parameters dataframe
  FSim_scenarios <- data.frame(
    flame_length = c(1, 3, 5, 7, 10, 20),
    fm1 = c(8, 7, 6, 5, 4, 3),
    fm10 = c(8, 7, 6, 5, 4, 4),
    fm100 = c(10, 9, 8, 7, 6, 5),
    fm1000 = c(15, 13, 11, 10, 8, 6),
    fmduff = c(50, 45, 40, 30, 20, 15),
    fmlwood = c(110, 100, 90, 80, 60, 50),
    fmlherb = c(110, 100, 90, 70, 40, 40),
    wspd_mph = c(2, 4, 4, 6, 7, 10),
    moisture = rep("",6),
    temp_F = c(85, 85, 85, 90, 90, 90),
    mortality = rep(1, 6),
    per_stand_burned = c(70, 80, 90, 90, 100, 100),
    season = rep(3, 6)
  )
  
  params_df <- build_params_df(combo_name, FSim_scenarios)
  if (is.null(params_df)) {
    message("No fire events for ", combo_name)
    return(NULL)
  }
  print(params_df)
  
  fire_kcp_dir <- paste0(RunDirectory, "/fire_kcps_", combo_name_safe)
  
  create_fire_kcp_files <- function(params_df, fire_kcp_dir, compute = TRUE) {
    dir.create(fire_kcp_dir)
    
    for (i in seq_len(nrow(params_df))) {
      p <- params_df[i, ]
      
      # Generate filenames for only the rows in this subset
      fire_kcp_names <- paste0("fire_yr_", params_df$fire_year, "_fl_", params_df$flame_length, ".kcp")
      
      # Output file path
      kcp_filename <- file.path(fire_kcp_dir, fire_kcp_names[i])
      
      # Build the base kcp text with hardcoded values
      kcp_text <- paste0(
        "!! Auto-generated fire KCP file based on scenario ", i, "\n",
        "!! Variables hard-coded from parameters\n\n",
        
        "*Keyword | Field 1 | Field 2 | Field 3 | Field 4 | Field 5 | Field 6 | Field 7 |\n",
        "* -------+---------+---------+---------+---------+---------+---------+---------+\n")
      
      # Add compute statement text only if applicable
      if(compute) {
        kcp_text <- c(kcp_text, 
                      paste0(
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
                        "ENDIF\n\n"
                        
                      ))
      }
      # Add the FMIN block for all KCPs
      kcp_text <- paste0(kcp_text,
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
      writeLines(kcp_text, con = kcp_filename)
    }
  }
  
  if (nrow(params_df) > 1) {
    message("Multiple fires detected. Using multi-fire KCP logic for later fires.")
    
    # First fire uses full compute version
    create_fire_kcp_files(params_df[1, , drop = FALSE], fire_kcp_dir, compute = TRUE)
    
    # Fires 2–5 use reduced compute version
    create_fire_kcp_files(params_df[-1, , drop = FALSE], fire_kcp_dir, compute = FALSE)
    
  } else {
    # Single fire only → full version
    create_fire_kcp_files(params_df, fire_kcp_dir, compute = TRUE)
  }


#Paths to fire kcps
fire_kcps <- list.files(paste0(fire_kcp_dir), full.names = TRUE)

createInputFile <- function(stand, managementID, outputDatabase, kcp_file, fire_kcps, inputDatabase) {
  # Helper to create ADDFILE block for any kcp file
  addfile_block <- function(kcp_path) {
    paste0(
      "OPEN              81\n",
      kcp_path, "\n",
      "ADDFILE           81\n",
      "CLOSE             81\n"
    )
  }
  
  # Fire kcps (loop over vector)
  fire_blocks <- paste(vapply(fire_kcps, addfile_block, FUN.VALUE = character(1)), collapse = "")
  
  
  paste0(
    'STDIDENT\n', stand, '\n',
    'STANDCN\n', stand, '\n',
    'MGMTID\n', managementID, '\n',
    'NUMCYCLE        5\n',
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
    'FIRECALC           0         1         2\n',
    'END\n',
    
    # ---- Tree outputs and classification ----
    # 'TreeList        1\n',
    # 'TreeList        2\n',
    # 'TreeList        3\n',
    'CALBSTAT\n',
    'CutList\n',
    'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
    
    # ---- ADDFILE links to shared .kcp ----
    
    fire_blocks,
    
    'Process\n\n'
  )
}
  
  
  #Make an outputs directory
  dir.create(paste0(RunDirectory, "/outputs"))
  
  outputDatabase <- paste0("./outputs/", run_name, ".db")
  
  #Connect to the database and extract the FVS_STANDINIT table
  con <- dbConnect(SQLite(), input_db)
  standinit <- dbReadTable(con, "FVS_STANDINIT")
  dbDisconnect(con)
  
  #Create the stand list - fields should be Stand_ID and Variant
  standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                      Variant = standinit$Variant,
                                      stringsAsFactors = FALSE)
  stands_to_run <- standlist %>% dplyr::filter(Stand_ID %in% stands)
  if (nrow(stands_to_run) == 0) {
    message("No matching Stand_IDs found in DB for combo ", combo_name, ". Skipping.")
    return(invisible(NULL))
  }
  
  key_text_all <- character()
  
  
  for(j in seq_len(nrow(stands_to_run))){
    st <- stands_to_run$Stand_ID[j]
    mgmtID <- paste0(combo_name_safe, "_", st)
    key_text_all <- c(key_text_all,
                      createInputFile(stand = st, 
                                      managementID = mgmtID,
                                      outputDatabase = outputDatabase,
                                      fire_kcps = fire_kcps,
                                      inputDatabase = input_db))
    
    #Append STOP
    key_text_all <- c(key_text_all, "STOP\n")
    
    # Write output
    key_file <- file.path(RunDirectory, paste0(combo_name_safe, ".key"))
    in_file <- file.path(RunDirectory, paste0(combo_name_safe, ".in"))
    
    writeLines(key_text_all, con = key_file)
    
    fvs_in <- paste0(
      combo_name_safe, ".key\n",
      combo_name_safe, ".fvs\n",
      combo_name_safe, ".out\n",
      combo_name_safe, ".trl\n",
      combo_name_safe, ".sum\n"
    )
    
    writeLines(fvs_in, con = in_file)
  }
  
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Build and set the keyword file command
    keyword_file <- paste0("--keywordfile=", combo_name_safe, ".key")
    print(keyword_file)
    
    rFVS::fvsSetCmdLine(
      cl = keyword_file,
      PACKAGE = paste0("FVS", variant)
    )
    
    # Run until done
    retCode <- 0
    while (retCode == 0) {
      retCode <- rFVS::fvsRun(PACKAGE = paste0("FVS", variant))
    }
  })
  
  list(variant = variant, status = retCode)
  
  message("Finished combo ", combo_name, " (variant=", variant, "). Output DB: ", outputDatabase)
  return(invisible(list(combo = combo_name, run_dir = RunDirectory, out_db = outputDatabase)))
  
}

# --- Parallel loop over all combos ---
plan(multisession, workers = 32)  

# --- Parallel job submission ---
future_map(names(standid_list), function(combo_name) {
  stands <- standid_list[[combo_name]]
  stands <- stands[!is.na(stands)]
  if(length(stands) == 0) {
    message("Skipping combo ", combo_name, " because all StandIDs are NA")
    return(NULL)
  }
  
  #Check whether the combination of events involves rx fire and wildfire in the same year (year 1)
  # If it does, set the wildfire event to NA
  combo_name <- no_yr1_fire_if_rx_fire(combo_name)
  
  # Parse combo name
  variant <- str_match(combo_name, "Var([^_]+)")[,2]
  treatment <- str_match(combo_name, "Trt(\\d+)")[,2]
  fire_events <- str_extract_all(combo_name, "Y\\d+:\\d+")[[1]]
  
  # Classify case
  case_type <- classify_case(treatment, fire_events)
  
  print(combo_name)
  print(stands)
  
  # Dispatch
  if (case_type == "no_fire_no_trt") run_no_fire_no_trt(combo_name, variant, stands)
  if (case_type == "trt_no_fire")    run_trt_no_fire(combo_name, variant, treatment, stands, get_kcp_path)
  if (case_type == "trt_with_fire")  run_trt_with_fire(combo_name, variant, treatment, fire_events, stands, get_kcp_path,
                                                       parse_fire_events, build_params_df, make_fire_filenames)
  if (case_type == "fire_no_trt")    run_fire_no_trt(combo_name, variant, fire_events, stands,
                                                     parse_fire_events, build_params_df, make_fire_filenames)
})
plan(sequential)

combine_dbs_recursive(wd)


