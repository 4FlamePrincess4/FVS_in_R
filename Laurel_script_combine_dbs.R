library(tidyverse) #For data manipulation
library(RSQLite) #For working with sqlite databases
library(DBI) #For interacting with different databases in R
library(data.table) #Faster data format, works with default R dataframe functions

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
      if (!is.null(df)) {
        df$db <- db_label  #Add a new column with the source database label to track which entries came from which database
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
