library(tidyverse)
library(RSQLite)
library(data.table)
library(stringr)

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

#Path to FlameAdjust kcp (generalized to be used for all .key scenarios generated here)
kcp_path <- "D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/OkaWen_kcps_v2_Eireann_feedback"

#Create a table of FSim fire scenario parameters (flame lengths and associated fuel moisture and weather conditions)
FSim_scenarios <- data.frame(
  flame_length = c(1, 3, 5, 7, 10, 20),
  fm1 = c(8, 7, 6, 5, 4, 3),
  fm10 = c(8, 7, 6, 5, 4, 4),
  # other variables like fm100, fm1000, fmduff, etc.
  wspd_mph = c(2, 2, 4, 6, 7, 10),
  temp_F = c(85, 85, 85, 90, 90, 90),
  mortality = rep(1, 6),
  per_stand_burned = c(70, 80, 90, 90, 100, 100),
  season = rep(3, 6)
)

#Modification of Rachel's function to generate .key files across FSim scenarios (previously dictated in multiple .kcp files)
createInputFile <- function(stand, managementID, params_row, outputDatabase, areaSpecificKcp, inputDatabase) {
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
    'WSPD = ', params_row$wspd_mph, '\n',
    'TEMP = ', params_row$temp_F, '\n',
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
    areaSpecificKcp, '\n',
    'ADDFILE           81\n',
    'CLOSE             81\n',

    # ---- SIMFIRE line from scenario ----
    'SIMFIRE           1        ',
    params_row$wspd_mph, '                ',
    params_row$temp_F, '        ',
    params_row$mortality, '       ',
    params_row$per_stand_burned, '         ',
    params_row$season, '\n',

    'Process\n\n'
  )
}

#Write keywords function using the above createInputFile function, iterating over FSim scenarios
write_keywords <- function(database_path, FSim_scenarios, kcp_path) {
  con <- dbConnect(SQLite(), database_path)
  standinit <- dbReadTable(con, "FVS_STANDINIT")
  dbDisconnect(con)

  for (i in seq_len(nrow(FSim_scenarios))) {
    row <- FSim_scenarios[i, ]
    fl_label <- paste0("FL", row$flame_length)

    for (j in seq_len(nrow(standinit))) {
      stand <- standinit$Stand_ID[j]
      variant <- standinit$Variant[j]
      key_name <- paste0(stand, "_", fl_label, "_", variant)
      out_db <- paste0("./outputs/", key_name, ".db")

      keywords <- createInputFile(
        stand = stand,
        managementID = key_name,
        params_row = row,
        outputDatabase = out_db,
        areaSpecificKcp = kcp_path,
        inputDatabase = database_path
      )

      key_file <- file.path(RunDirectory, paste0(key_name, ".key"))
      in_file <- file.path(RunDirectory, paste0(key_name, ".in"))
      
      writeLines(keywords, key_file)
      
      fvs_in <- paste0(
        key_name, ".key\n",
        key_name, ".fvs\n",
        key_name, ".out\n",
        key_name, ".trl\n",
        key_name, ".sum\n"
      )
      
      writeLines(fvs_in, in_file)
    }
  }
}

#Write the .key files using the above functions
write_keywords(EC_dbs, FSim_scenarios, kcp_path)
