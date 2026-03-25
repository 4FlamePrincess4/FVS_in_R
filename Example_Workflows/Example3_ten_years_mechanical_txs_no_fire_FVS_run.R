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

setwd("C:/Users/Laurel/Documents/WFSETP/Scenarios/Maximum_Effort/FVS/FVS_Run3_tx_yr2_no_wildfire_10years")
wd <- getwd()

#Set the FVS executable folder
fvs_bin = "C:/FVS/FVSSoftware/FVSbin"

#---Set location of TMFM sqlite databases and read in data----------------------
TMFM2020_dir_path <- "C:/Users/Laurel/Documents/WFSETP/Scenarios/Maximum_Effort/FVS/TMFM_2020_OkaWen_Databases/"

okawen_dbs <- list.files(TMFM2020_dir_path,
                         full.names=TRUE)
#We only need to run this for the East and West Cascades variants
okawen_dbs <- okawen_dbs[c(1,4)]

#Name the FVS run
run_name <- str_c("treat_no_fire_ten_years_ec_wc_variants",
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

#Paths to kcps
treat_kcps <- list.files(paste0(RunDirectory, "/treat_kcps"))
kcp_dir <- paste0(RunDirectory, "/treat_kcps")

#Modification of Rachel's function to generate .key files across FSim scenarios (previously dictated in multiple .kcp files)
createInputFile <- function(stand, managementID, outputDatabase, treat_kcp, inputDatabase) {
  paste0(
    'STDIDENT\n', stand, '\n',
    'STANDCN\n', stand, '\n',
    'MGMTID\n', managementID, '\n',
    'NUMCYCLE        10\n',
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
    'FIRECALC           0         1        1\n',
    'STATFUEL\n',
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
    
    'Process\n\n'
  )
}

write_keywords <- function(database_path){
  #Connect to the database and extract the FVS_STANDINIT table
  con <- dbConnect(SQLite(), database_path)
  standinit <- dbReadTable(con, "FVS_STANDINIT")
  dbDisconnect(con)
  
  #Iterate over flame length classes
  # (Later you can adapt Rachel's code to list the FlameAdjust kcps to iterate over)
  #for(fl in seq_along(FL)){
  #Iterate over the KCP files
  for(kcp in seq_along(treat_kcps)){
    #Get the root kcp label
    kcp_name <- stringr::str_sub(treat_kcps[kcp], end = -5) 
    #Create the stand list - fields should be Stand_ID, Variant, and kcp
    standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                        Variant = standinit$Variant,
                                        kcp = rep(paste0(treat_kcps[kcp]), nrow(standinit)))
    #Create a key label for each unique combination of kcp & variant
    treat_key <- paste0(kcp_name, "_", standinit$Variant[1])
    
    #assign the full kcp path
    areaSpecificKcp <- paste0(kcp_dir, "/", treat_kcps[kcp])
    
    outputDatabase <- paste0("./outputs/", treat_key, ".db")
    
    #Create keywords for each unique combination of kcp & variant
    keywords <- createInputFile(stand = standlist$Stand_ID, 
                                managementID = standlist$Stand_ID, 
                                outputDatabase = outputDatabase, 
                                treat_kcp = areaSpecificKcp, 
                                inputDatabase = database_path)
    
    # Append STOP
    keywords <- c(keywords, "STOP\n")
    
    #Print to the key files
    write(keywords, file = paste0(RunDirectory, "/", treat_key, ".key"))
    
    #Create the .in file
    fvs_in <- paste0(treat_key, ".key\n",
                     treat_key, ".fvs\n",
                     treat_key, ".out\n",
                     treat_key, ".trl\n",
                     treat_key, ".sum\n")
    fvs_in_file <- paste0(RunDirectory, '\\', treat_key, '.in')
    write(fvs_in, file = fvs_in_file)
  } #End loop over kcp files
  #} #End loop over flame lengths  
} #End function to write key and in files

#Write the scenario .key files in parallel
lapply(okawen_dbs, write_keywords)

#Create the data frame of run scenario parameters to parallelize over
#The number of rows is the number of FVS runs you're doing
variants <- c("ec", "wc")
treatments <- list.files("./treat_kcps")
treatments <- stringr::str_sub(treatments, end = -5) 
runs <- expand.grid(variant=variants, treatment = treatments, 
                    stringsAsFactors = FALSE)
print(runs)

#Remove the rx fire ones
runs <- runs[-c(7,8),]

#Make an outputs directory
dir.create(paste0(RunDirectory, "/outputs"))

runFVS <- function(variant, treatment, RunDirectory, fvs_bin) {
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", variant, ".dll"),
      bin = fvs_bin
    )
    
    # Set the command line (keyword file)
    print(paste0("--keywordfile=", treatment, "_", variant, ".key"))
    rFVS::fvsSetCmdLine(
      cl = paste0("--keywordfile=",treatment, "_", variant, ".key"),
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
    treatment = treatment,
    status = retCode
  )
}

n_runs <- nrow(runs)
future::plan(multisession, workers = n_runs)
future::plan(multisession, workers = 25) #there are 30 runs so I'm going to set this lower to avoid crashing my computer

#Run FVS in parallel for all combinations
furrr::future_pmap(
  list(
    variant = runs$variant,
    treatment=runs$treatment
  ),
  runFVS,
  RunDirectory = RunDirectory,
  fvs_bin = fvs_bin
)

plan(sequential)
gc()

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
