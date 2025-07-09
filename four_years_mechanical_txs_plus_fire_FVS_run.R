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
      
      "!! Legacy comments: significantly modified by AJM 12/22/10, modified April 2011\n",
      "!! Crown Percent consumed being modified so that it is no longer all-or-nothing\n",
      "!! (i.e. heretofore, CPC was 100% if FL was > FLCRIT, and 0 if FL was < CRITFL\n",
      "!! this new method sets CPC to 0.1 when FL=CRITFL and maxes it out to 100% when FL=30% of top ht.\n",
      "!! FL_ is flame length in feet and is set to the midpoint of the class.\n",
      "!! FLCLASS is an integer 1-20, assumed to be FL in 0.5 m increments (e.g. FLCLASS 10=5 meter\n",
      "!! the midpoint of the class 4.5-5.0 meters is 4.75 m = 15.58'\n\n",
      
      "!! FOR % Crowning:\n",
      "!! 1) calculate linear function defined by 2 x,y points (where x= FL, y=% crown consumed)such that\n",
      "!!    x1,y1 = crit FL, 0.1\n",
      "!!    x2,y2 = 30% of top ht, 1.0\n",
      "!! 2) transform y values to SQRT(y)\n",
      "!! 3) range of y (proportion consumed) is now 0.316 (when FL=crit FL) to 100% (when FL= 30% of top ht)\n",
      "!!    The function is concave downward.\n",
      "* FL is the variable being incremented\n\n",
      
      "*Keyword | Field 1 | Field 2 | Field 3 | Field 4 | Field 5 | Field 6 | Field 7 |\n",
      "* -------+---------+---------+---------+---------+---------+---------+---------+\n",
      "!! note: variable FLCLASS is assigned by ArcFuels keywriter via Landscape-->FVS Analysis-->FLP Specific\n\n",
      
      "COMPUTE           1\n",
      "TmpHt2Lv = -1\n",
      "TmpCBD = -1\n",
      "END\n\n",
      
      "COMPUTE           0\n\n",
      "FLEN = ", p$flame_length, "\n",
      
      "!! CPC is initially set to zero to be used if stands have very low CBD\n",
      "CPC = 0  \n\n",
      
      "PCHt2Lv = TmpHt2Lv\n",
      "!! this required COMPUTE is done elsewhere in arcfuels--> CBH=CRBASEHT\n",
      "TmpHt2Lv = CBH\n\n",
      
      "PCCBD=TmpCBD\n",
      "!!THIS REQUIRED COMPUTE IS DONE ELSEWHERE IN ARCFUELS : --> CBD=CRBULKDN\n",
      "TmpCBD = CBD\n\n",
      
      "!! SCORCH HEIGHT: convert flame length to scorch height using Van Wagner\n",
      "HSM = 6.026 * (FLEN / 3.2808) ** 1.4466\n",
      "HS_ = HSM * 3.2808\n",
      "END\n\n",
      
      "IF                0\n",
      "PCCBD GT 0.0001\n",
      "THEN\n",
      "COMPUTE           0\n",
      "Io_ = (0.010 * PCHt2Lv * 0.3048 * (460.0 + 25.9 * 100)) ** (3 / 2)\n",
      "FLCRIT = (0.07749 * Io_ ** 0.46) * 3.281\n\n",
      
      "FLCRIT2 = 0.3 * BTOPHT\n",
      "FSLOPE = 0.9 / (FLCRIT2 - FLCRIT)\n",
      "NTERCEPT = 1.0 - (FSLOPE * FLCRIT2)\n\n",
      
      "!! INTERCEPT MAY BE NEGATIVE, HENCE Y_ MAY BE NEGATIVE AT LOW FLENs\n",
      "!! HENCE CONSTRAIN Y_ TO BE NO SMALLER THAN ZERO\n\n",
      
      "Y_ = MAX((FLEN * FSLOPE + NTERCEPT), 0.0)\n\n",
      
      "YSQRT = SQRT(Y_)\n\n",
      
      "!! find out where FLEN is in relation to FLCRIT\n",
      "!! if FLEN is less than CFL, then CFL is > FLEN, set CPC to zero\n",
      "!! else use the function and bound it to a max of 1\n\n",
      
      "fplace = maxindex(FLEN, FLCRIT)\n",
      "CPC = INDEX(fplace,MIN(YSQRT,1),0)\n\n",
      
      "FDIFF = FLEN - FLCRIT\n",
      "!! CPC = LININT(FDIFF,0,0,0,100)\n",
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
runs <- expand.grid(variant=variants, flame_length = flame_lengths, treatment = treatments, 
                    stringsAsFactors = FALSE)
fire_kcp_df <- data.frame(
  flame_length = as.numeric(gsub("[^0-9]", "", basename(fire_kcps))),
  fire_kcp = fire_scenarios
)
runs <- dplyr::left_join(runs, fire_kcp_df, by = "flame_length")
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

#Now let's combine the databases on the other end
#Let's adapt Chris's function to be more generalizable across FVS runs regardless of the database outputs
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
#database_dir <- "D:/WFSETP/Scenario_Development/Treatment_Runs_FVS/treat_plus_fire_08Jul25_1201/outputs"

combine_dbs_general(RunDirectory = database_dir, rm.files = FALSE, output_db_name = "Combined_Outputs_All_FLs_txs.db")
