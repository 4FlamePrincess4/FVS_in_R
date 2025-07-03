combine_dbs <- function(RunDirectory, rm.files = F) {
  list_dbs <- list.files(RunDirectory, pattern = "*.db", full.names = T)
  list_dbs <- list_dbs[!str_detect(list_dbs, "All_FVSOut")]
allframes <- lapply(list_dbs, function(fn) {
  con <- DBI::dbConnect(RSQLite::SQLite(), fn)
  
  FVS_Cases <- tryCatch(DBI::dbReadTable(con, "FVS_Cases"), error = function(e) NULL)
  FVS_Error <- tryCatch(DBI::dbReadTable(con, "FVS_Error"), error = function(e) NULL)
  FVS_Compute <- tryCatch(DBI::dbReadTable(con, "FVS_Compute"), error = function(e) NULL)
  FVS_Summary2 <- tryCatch(DBI::dbReadTable(con, "FVS_Summary2"), error = function(e) NULL)
  FVS_Potfire <- tryCatch(DBI::dbReadTable(con, "FVS_Potfire"), error = function(e) NULL)
  FVS_CanProfile <- tryCatch(DBI::dbReadTable(con, "FVS_CanProfile"), error = function(e) NULL)
  FVS_Fuels <- tryCatch(DBI::dbReadTable(con, "FVS_Fuels"), error = function(e) NULL)
  FVS_BurnReport <- tryCatch(DBI::dbReadTable(con, "FVS_BurnReport"), error = function(e) NULL)
  FVS_Consumption <- tryCatch(DBI::dbReadTable(con, "FVS_Consumption"), error = function(e) NULL)
  FVS_Mortality <- tryCatch(DBI::dbReadTable(con, "FVS_Mortality"), error = function(e) NULL)
  FVS_Carbon <- tryCatch(DBI::dbReadTable(con, "FVS_Carbon"), error = function(e) NULL)
  FVS_Down_Wood_Cov <- tryCatch(DBI::dbReadTable(con, "FVS_Down_Wood_Cov"), error = function(e) NULL)
  FVS_StrClass <- tryCatch(DBI::dbReadTable(con, "FVS_StrClass"), error = function(e) NULL)
  FVS_Potfire_Cond <- tryCatch(DBI::dbReadTable(con, "FVS_Potfire_Cond"), error = function(e) NULL)
  FVS_Hrv_Carbon <- tryCatch(DBI::dbReadTable(con, "FVS_Hrv_Carbon"), error = function(e) NULL)
  FVS_Regen_Sprouts <- tryCatch(DBI::dbReadTable(con, "FVS_Regen_Sprouts"), error = function(e) NULL)
  FVS_Regen_Tally <- tryCatch(DBI::dbReadTable(con, "FVS_Regen_Tally"), error = function(e) NULL)
  FVS_TreeList <- tryCatch(DBI::dbReadTable(con, "FVS_TreeList"), error = function(e) NULL)
  FVS_Cutlist  <- tryCatch(DBI::dbReadTable(con, "FVS_Cutlist"), error = function(e) NULL)
  FVS_CalibStats  <- tryCatch(DBI::dbReadTable(con, "FVS_CalibStats"), error = function(e) NULL)
    
  DBI::dbDisconnect(con)
  list(
    FVS_Cases = FVS_Cases,
    FVS_Error = FVS_Error,
    FVS_Compute = FVS_Compute,
    FVS_Summary2 = FVS_Summary2,
    FVS_Potfire = FVS_Potfire,
    FVS_CanProfile = FVS_CanProfile,
    FVS_Fuels = FVS_Fuels,
    FVS_Carbon = FVS_Carbon,
    FVS_Down_Wood_Cov = FVS_Down_Wood_Cov,
    FVS_StrClass = FVS_StrClass,
    FVS_Hrv_Carbon = FVS_Hrv_Carbon,
    FVS_Regen_Sprouts = FVS_Regen_Sprouts,
    FVS_Regen_Tally = FVS_Regen_Tally,
    FVS_Mortality = FVS_Mortality,
    FVS_Potfire_Cond = FVS_Potfire_Cond,
    FVS_BurnReport = FVS_BurnReport,
    FVS_Consumption = FVS_Consumption,
    FVS_Cutlist = FVS_Cutlist,
    FVS_CalibStats = FVS_CalibStats,
    FVS_TreeList = FVS_TreeList
    )
})

table_names <- c("FVS_Cases", "FVS_Error", "FVS_Compute", "FVS_Summary2", "FVS_CalibStats",
                 "FVS_Potfire", "FVS_Potfire_Cond", "FVS_Fuels", "FVS_BurnReport", "FVS_Consumption", "FVS_Mortality", "FVS_Carbon", 
                 "FVS_CanProfile", "FVS_Down_Wood_Cov", "FVS_StrClass", "FVS_Hrv_Carbon", 
                 "FVS_Regen_Sprouts", "FVS_Regen_Tally", 
                 "FVS_TreeList", "FVS_Cutlist")

con <- DBI::dbConnect(RSQLite::SQLite(), str_c(RunDirectory, "/Full_Set_FVSOut.db"))
tbl <- "FVS_CalibStats"
# Loop through each table name and write to new DB
for (tbl in table_names) {
  # Extract data from allframes for this table
  tbl_list <- lapply(allframes, function(x) x[[tbl]])

  # Drop NULLs if any DB didn’t have the table
  tbl_list <- tbl_list[!sapply(tbl_list, is.null)]
  
  new_tbl <- data.table::rbindlist(tbl_list, fill = TRUE)
  # Only write if there's something to write
  if (length(tbl_list) > 0) {
    if(nrow(new_tbl)>0){
      DBI::dbWriteTable(con, tbl, new_tbl)
  }}
}
DBI::dbGetQuery(con, "select count(*) as n from FVS_Cases")

DBI::dbDisconnect(con)


if (rm.files == T) {
# Clean up the individual databases.
file.remove(list_dbs)
}


rm(allframes)
gc()

}
