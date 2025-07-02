library(tidyverse) #For data manipulation
library(RSQLite) #For working with sqlite databases
library(data.table) #Faster data format, works with default R dataframe functions

#---Set location of TMFM sqlite databases and read in data----------------------
TMFM2020_dir_path <- "D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/TMFM_2020_OkaWen_Databases"

okawen_dbs <- list.files(TMFM2020_dir_path,
                         full.names=TRUE)
#We only need to run this for the East Cascades variant
EC_dbs <- okawen_dbs[1]

#Name the FVS run
run_name <- str_c("no_treat_",
                  strftime(Sys.Date(), "%d%b%y"),
                  "_", strftime(Sys.time(), "%H%M"))

# Create a dir for this run.
RunDirectory <- str_c(wd, "/", run_name)
dir.create(RunDirectory)

#List the KCP files
kcp_dir <- "D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/OkaWen_kcps_v2_Eireann_feedback"
kcp_paths <- list.files(kcp_dir)

#Set flame lengths for Rachel's FVS compute function inside the createInputFile function
FL <- c("1", "3", "5", "7", "10", "20", "0")
FL <- 3

#-------Rachel's function to create the string for each stand in FVS-------
createInputFile <- function(stand, managementID, FL, outputDatabase, areaSpecificKcp, inputDatabase){
  # Create .key file
  input <- paste0('STDIDENT\n',
                  stand, '\n',
                  'STANDCN\n',
                  stand, '\n',
                  'MGMTID\n',
                  managementID,
                  '\nNUMCYCLE       3\n',
                  'TIMEINT         0        1\n',
                  'SCREEN\n',
                  'DataBase\n',
                  'DSNout\n',
                  outputDatabase, '\n',
                  'SUMMARY\n',
                  'COMPUTDB\n',
                  'CALBSTDB\n',
                                    
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
                  
                  'DATABASE\n',
                  'DSNIN\n',
                  inputDatabase, '\n',
                  'StandSQL\n',
                  'SELECT * FROM FVS_StandInit\n',
                  "WHERE  Stand_ID  = '%stand_cn%'\n",
                  'EndSQL\n',
                  'DSNIN\n',
                  inputDatabase, '\n',
                  'TreeSQL\n',
                  'SELECT * FROM FVS_TreeInit\n',
                  'WHERE Stand_ID = (SELECT Stand_ID FROM FVS_StandInit\n',
                  "WHERE Stand_ID = '%stand_cn%')\n",
                  'EndSQL\n',
                  'END\n',
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
                  'FLEN = ', FL, '\n',
                  '\n',
                  'END\n',

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
                  
                  'TreeList        1\n',
                  'TreeList        2\n',
                  'CALBSTAT\n',
                  'CutList\n',
                  'STRCLASS           1     30.00        5.       16.     20.00       50.     35.00\n',
                  
                  ## Include any addfiles here.
                  'OPEN              81\n',
                  areaSpecificKcp,
                  "\n",
                  'ADDFILE           81\n',
                  'CLOSE             81\n',
                  'Process\n\n')
}

#Create the output database file path from Rachel: "database MUST exist before running FVS"
outputDatabase <- paste0("./outputs/", run_name, ".db")

#------Version where you loop over databases and, optionally, flame length classes-------
write_keywords <- function(database_paths){
  for(db in seq_along(database_paths)){
    #Connect to the database and extract the FVS_STANDINIT table
    con <- dbConnect(SQLite(), database_paths[db])
    standinit <- dbReadTable(con, "FVS_STANDINIT")
    dbDisconnect(con)
    
    #Iterate over flame length classes
    # (Later you can adapt Rachel's code to list the FlameAdjust kcps to iterate over)
    #for(fl in seq_along(FL)){
      #Iterate over the KCP files
      for(kcp in seq_along(kcp_paths)){
        #Get the root kcp label
        kcp_name <- stringr::str_sub(kcp_paths[kcp], end = -5) 
        #Create the stand list - fields should be Stand_ID, Variant, and kcp
        standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                            Variant = standinit$Variant,
                                            kcp = rep(paste0(kcp_paths[kcp]), nrow(standinit)))
        #Create a key label for each unique combination of kcp & variant
        treat_key <- paste0(kcp_name, "_", standinit$Variant[1])
        
        #assign the full kcp path
        areaSpecificKcp <- paste0(kcp_dir, "/", kcp_paths[kcp])
        
        #Create keywords for each unique combination of kcp & variant
        keywords <- createInputFile(stand = standlist$Stand_ID, 
                                    managementID = standlist$Stand_ID, 
                                    #FL = FL[fl], 
                                    FL = 3,
                                    outputDatabase = outputDatabase, 
                                    areaSpecificKcp = areaSpecificKcp, 
                                    inputDatabase = database_paths[db])
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
  #  } #End loop over flame lengths  
  } #End loop over database paths
} #End function to write key and in files

#-------Version where you don't have to loop over the databases or flame lengths-------
write_keywords <- function(database_path){
    #Connect to the database and extract the FVS_STANDINIT table
    con <- dbConnect(SQLite(), database_path)
    standinit <- dbReadTable(con, "FVS_STANDINIT")
    dbDisconnect(con)
    
    #Iterate over flame length classes
    # (Later you can adapt Rachel's code to list the FlameAdjust kcps to iterate over)
    #for(fl in seq_along(FL)){
    #Iterate over the KCP files
    for(kcp in seq_along(kcp_paths)){
      #Get the root kcp label
      kcp_name <- stringr::str_sub(kcp_paths[kcp], end = -5) 
      #Create the stand list - fields should be Stand_ID, Variant, and kcp
      standlist <- data.table::data.table(Stand_ID = standinit$Stand_ID,
                                          Variant = standinit$Variant,
                                          kcp = rep(paste0(kcp_paths[kcp]), nrow(standinit)))
      #Create a key label for each unique combination of kcp & variant
      treat_key <- paste0(kcp_name, "_", standinit$Variant[1])
      
      #assign the full kcp path
      areaSpecificKcp <- paste0(kcp_dir, "/", kcp_paths[kcp])
      
      #Create keywords for each unique combination of kcp & variant
      keywords <- createInputFile(stand = standlist$Stand_ID, 
                                  managementID = standlist$Stand_ID, 
                                  #FL = FL[fl], 
                                  FL = 3,
                                  outputDatabase = outputDatabase, 
                                  areaSpecificKcp = areaSpecificKcp, 
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

write_keywords(EC_dbs)
