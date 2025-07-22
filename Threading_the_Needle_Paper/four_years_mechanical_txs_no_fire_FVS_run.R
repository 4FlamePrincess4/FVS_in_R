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
run_name <- str_c("treat_no_fire_",
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
    
    #Create keywords for each unique combination of kcp & variant
    keywords <- createInputFile(stand = standlist$Stand_ID, 
                                managementID = standlist$Stand_ID, 
                                outputDatabase = outputDatabase, 
                                treat_kcp = areaSpecificKcp, 
                                inputDatabase = database_path)
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
write_keywords(EC_dbs)

#Create the data frame of run scenario parameters to parallelize over
#The number of rows is the number of FVS runs you're doing
variants <- "ec"
treatments <- list.files("./treat_kcps")
treatments <- stringr::str_sub(treatments, end = -5) 
runs <- expand.grid(variant=variants, treatment = treatments, 
                    stringsAsFactors = FALSE)
print(runs)

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