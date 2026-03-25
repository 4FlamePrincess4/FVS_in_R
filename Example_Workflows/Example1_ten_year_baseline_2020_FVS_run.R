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

setwd("C:/Users/Laurel/Documents/WFSETP/Scenarios/Maximum_Effort/FVS/FVS_Run1_no_dist_10years")
wd <- getwd()

#Set the FVS executable folder
fvs_bin = "C:/FVS/FVSSoftware/FVSbin"

#---Set location of TMFM sqlite databases and read in data----------------------
TMFM2020_dir_path <- "C:/Users/Laurel/Documents/WFSETP/Scenarios/Maximum_Effort/FVS/TMFM_2020_OkaWen_Databases/"

okawen_dbs <- list.files(TMFM2020_dir_path,
                         full.names=TRUE)

#okawen_dbs <- okawen_dbs[c(1,4)]

#Name the FVS run
run_name <- str_c("baseline_2020_FVS_run_all_variants",
                  strftime(Sys.Date(), "%d%b%y"),
                  "_", strftime(Sys.time(), "%H%M"))

# Create a dir for this run.
RunDirectory <- str_c(wd, "/", run_name)
dir.create(RunDirectory)

createInputFile_no_trt <- function(stand, managementID, outputDatabase, inputDatabase) {
  paste0(
    'STDIDENT\n', stand, '\n',
    'STANDCN\n', stand, '\n',
    'MGMTID\n', managementID, '\n',
    'NUMCYCLE       10\n',
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
    'FIRECALC           0         1         1\n',
    'STATFUEL\n',
    'END\n',
    
    # ---- Tree outputs and classification ----
    'TreeList        1\n',
    'TreeList        2\n',
    'TreeList        3\n',
    'CALBSTAT\n',
    'CutList\n',
    'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
    
    'Process\n\n'
  )
}

write_keywords_no_trt <- function(database_paths){
  for(db in seq_along(database_paths)){
    #Connect to the database and extract the FVS_STANDINIT table
    con <- dbConnect(SQLite(), database_paths[db])
    standinit <- dbReadTable(con, "FVS_STANDINIT")
    dbDisconnect(con)
    
    #Create the stand list - fields should be Stand_ID, Variant, and kcp
    standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                        Variant = standinit$Variant)
    #Create a key label 
    treat_key <- paste0("baseline2020_", standinit$Variant[1])
    
    outputDatabase <- paste0("./outputs/", treat_key, ".db")
    
    #Character vector to store the key file text before appending STOP
    key_text_all <- character()
    
    for(j in seq_len(nrow(standinit))){
      key_text_all <- c(
        #Create keywords for each unique combination of kcp & variant
        #Append to the character vector
        createInputFile_no_trt(stand = standlist$Stand_ID, 
                               managementID = standlist$Stand_ID, 
                               outputDatabase = outputDatabase, 
                               inputDatabase = database_paths[db])
      )
    }
    
    #Append STOP
    key_text_all <- c(key_text_all, "STOP\n")
    
    # Write output
    key_file <- file.path(RunDirectory, paste0(treat_key, ".key"))
    in_file <- file.path(RunDirectory, paste0(treat_key, ".in"))
    
    writeLines(key_text_all, key_file)
    
    fvs_in <- paste0(
      treat_key, ".key\n",
      treat_key, ".fvs\n",
      treat_key, ".out\n",
      treat_key, ".trl\n",
      treat_key, ".sum\n"
    )
    
    writeLines(fvs_in, in_file)
  } #End loop over database paths
} #End function to write key and in files

#Write the .key files using the above functions
write_keywords_no_trt(okawen_dbs)

#Make an outputs directory
dir.create(paste0(RunDirectory, "/outputs"))

runFVS <- function(variant, RunDirectory, fvs_bin) {
  withr::with_dir(RunDirectory, {
    # Load the variant DLL
    rFVS::fvsLoad(
      fvsProgram = paste0("FVS", toupper(variant), ".dll"),
      bin = fvs_bin
    )
    
    # Build and set the keyword file command
    keyword_file <- paste0("--keywordfile=baseline2020_", toupper(variant), ".key")
    print(keyword_file)
    
    rFVS::fvsSetCmdLine(
      cl = keyword_file,
      PACKAGE = paste0("FVS", toupper(variant))
    )
    
    # Run until done
    retCode <- 0
    while (retCode == 0) {
      retCode <- rFVS::fvsRun(PACKAGE = paste0("FVS", toupper(variant)))
    }
  })
  
  list(variant = variant, status = retCode)
}

variants <- c("ec", "wc", "PN", "IE")
variants <- as.data.frame(variants)

#Plan for parallel execution
n_runs <- nrow(variants)
##NOTE: if n_runs exceeds the number of logical processors on your machine, set the workers equal to the number of logical processors. Setting the number of workers too high may crash R.
future::plan(multisession, workers = 25)

#Run FVS in parallel for all combinations
furrr::future_pmap(
  list(
    variant = variants$variant
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
