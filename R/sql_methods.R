#' Execute SQL SELECT statements from Nuvolos.cloud
#'
#' Function read_sql(sql, dbname, schemaname, parse_dates).
#' Executes SELECT SQL statements in the connected Nuvolos schema.
#' On Nuvolos the database and schema are by default the ones the user is working in, from local machine they need to be specified.
#' 
#' @param sql SQL statement to be executed. Note that quoting the tables is needed only if the table name is case sensitive (it contains both upper and lowercase letters or special chars).
#' @param dbname The name of the database from which the SELECT statement will be executed.
#' @param schemaname The name of the schema from which the SELECT statement will be executed.
#' @return Returns an R dataframe object. When date format columns are in the table, they are returned as Date. 
#' 
#' @examples
#' db <- read_sql("SELECT * FROM table")
#' db <- read_sql("SELECT * FROM table", dbname = "space_1", schemaname = "test_schema")
#' @export
read_sql <- function(sql, dbname = NULL, schemaname = NULL){


 # importing necessary python packages
 nuvolos <- import_nuvolos() 
 pd <- reticulate::import("pandas")

 username <- NULL
 password <- NULL
 
 # reading credentials for establishing connection
 conn_param <- get_credentials(username, password, dbname, schemaname)
 
 username <- conn_param[['username']]
 password <- conn_param[['password']]
 dbname  <- conn_param[['dbname']]
 schemaname <- conn_param[['schemaname']]
 
 # creating engine and establishing connection with python-based nuvolos connector
 engine <- nuvolos$get_engine(username = username,
                               password = password,
                               dbname = dbname,
                               schemaname = schemaname)
 con <- engine$connect()
 
 # using python's pandas.read_sql() method execute select query. 
 # After execution the connection is closed and the engine is disposed.
 tryCatch({
   result <- pd$read_sql(sql, con)
 }, finally = {
   con$close()
   engine$dispose()
 })
 
 # Unlisting list column types. Also substituting NULL values to NA to remain consistent.
 for (i in seq(1,ncol(result))){
   if (typeof(result[,i]) == "list"){
     if (class(result[,i][[1]])[1] == "datetime.date"){
       
       # returning dates in posixct format if stored as dates.
       result[,i] <- as.Date(unlist(lapply(result[,i], function(x) {if (is.null(x)){NA} else {as.character(x)}})))
     }
     else {
     result[,i] <- unlist(lapply(result[,i], function(x) {if (is.null(x)){NA} else {x}}))
     }
   }
 }
 
 return(result)
}


#' Write tables to Nuvolos.cloud
#'
#' Function to_sql(df, name, dbname, schemaname, if_exists, index, index_label, nanoseconds).
#' Creates table in the connected nuvolos schema from an R dataframe.
#' On Nuvolos the database and schema are by default the ones the user is working in, from local machine they need to be specified.
#' The function supports bulk loading.
#' 
#' @param df Name of the R dataframe to be written to a table.
#' @param name The name of the database table. It will only be quoted and case sensitive if it contains keywords or special chars
#' @param dbname The name of the database to which data will be inserted.
#' @param schemaname The name of the schema to which data will be inserted.
#' @param if_exists How to behave if the table already exists. {‘fail’, ‘replace’, ‘append’}, default ‘fail’
#' \itemize{
#' \item fail: Raise a ValueError.
#' \item replace: Drop the table before inserting new values.
#' \item append: Insert new values to the existing table.
#' }
#' @param index bool, default True: Write DataFrame index as a column. Uses index_label as the column name in the table.
#' @param index_label column label for index column(s). If None is given (default) and index is True, then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
#' @param nanoseconds if True, nanosecond timestamps will be used to upload the data. Limits timestamp range from 1677-09-21 00:12:43.145224192 to 2262-04-11 23:47:16.854775807.
#'
#' @examples
#' to_sql(df = df, name = "table", if_exists = 'replace', index = FALSE)
#' to_sql(df = df, name = "table", dbname = "space_1", schemaname = "test_schema", if_exists = 'replace', index = FALSE)
#'
#' @export
to_sql <- function(df,
                   name,
                   dbname=NULL,
                   schemaname=NULL,
                   if_exists="fail",
                   index=TRUE,
                   index_label=NULL,
                   nanoseconds=FALSE){


  # importing necessary python package
  nuvolos <- import_nuvolos()
  
  username <- NULL
  password <- NULL

  # reading credentials for establishing connection
  conn_param <- get_credentials(username, password, dbname, schemaname)
  
  username <- conn_param[['username']]
  password <- conn_param[['password']]
  dbname  <- conn_param[['dbname']]
  schemaname <- conn_param[['schemaname']]
  
  # creating engine and establishing connection with python-based nuvolos connector
  reticulate::py_run_string(paste("from nuvolos import get_engine; engine = get_engine(dbname = ","'", dbname,"'", ", schemaname = ", "'", schemaname, "'", ", username = ", "'", username, "'", ", password = ", "'", password, "'", ")", sep = ""))
  reticulate::py_run_string("con = engine.connect()")
  
  # using nuvolos.to_sql function to create table in the selected database and schema.
  # After execution the connection is closed and the engine is disposed.

  # writing file to temporary directory in feather format, which is used as an intermediary
  #between r and python, in order to handle null and NA values. The temporary file is deleted 
  #when the function exits. 
  tf <- tempfile()
  on.exit(unlink(tf))
  arrow::write_feather(df, tf)
  
  tryCatch({
    reticulate::py_run_string(paste("import pandas as pd;from nuvolos import to_sql;df = pd.read_feather('", tf, "');to_sql(df, ", "'", name, "'", ", database = ","'", dbname,"'", ", schema = ", "'", schemaname, "'", ", if_exists = 'replace', index = False, con = con)",  sep = ""))
  }, finally = {
    reticulate::py_run_string("con.close()")
    reticulate::py_run_string("engine.dispose()")
  })
}


#' Execute any SQL statement from Nuvolos.cloud
#'
#' Function execute(sql, dbname, schemaname).
#' Executes any SQL statement in the connected Nuvolos schema.
#' On Nuvolos the database and schema are by default the ones the user is working in, from local machine they need to be specified.
#' 
#' @param sql SQL statement to be executed. Note that quoting the tables is needed only if the table name is case sensitive (it contains both upper and lowercase letters or special chars).
#' @param dbname The name of the database from/in which the statement will be executed.
#' @param schemaname The name of the schema from/in which the statement will be executed.
#' @return Returns the result of python's execute method.
#' 
#' @examples
#' execute("DROP TABLE table")
#' execute("DROP TABLE table", dbname = "space_1", schemaname = "test_schema")

#' @export
execute <- function(sql, dbname = NULL, schemaname = NULL){

  # importing necessary python package
  nuvolos <- import_nuvolos()
  
  username <- NULL
  password <- NULL
  
  # reading credentials for establishing connection
  conn_param <- get_credentials(username, password, dbname, schemaname)
  
  username <- conn_param[['username']]
  password <- conn_param[['password']]
  dbname  <- conn_param[['dbname']]
  schemaname <- conn_param[['schemaname']]
  
  # creating engine and establishing connection with python-based nuvolos connector
  engine <- nuvolos$get_engine(username = username,
                               password = password,
                               dbname = dbname,
                               schemaname = schemaname)
  con <- engine$connect()
  
  # using python's execute method on the established connection.
  # After execution the connection is closed and the engine is disosed.
  tryCatch({
    con$execute(sql)
  }, finally = {
    con$close()
    engine$dispose()
  })

}


import_nuvolos <- function(){
  
  # importing nuvolos python connector, installing if not available
  nuvolos <- tryCatch({
    reticulate::import("nuvolos")
  }, error = function(e){
    reticulate::py_install("nuvolos", pip = TRUE)
    return(reticulate::import("nuvolos"))
  })
  
  return(nuvolos)
}

get_credentials <- function(username, password, dbname, schemaname){
  # checking whether user is on Nuvolos, asking for credentials if not
  if (is_local()){
    return(get_local_info(username, password, dbname, schemaname))
  } else {
    cred <- get_nuvolos_info(username, password, dbname, schemaname)
    # getting rid of the extra quotation marks around the dbname and schemaname
    cred[3:4] <-lapply(cred[3:4], function(x) if (length(unlist(strsplit(x,'"')))>1) {unlist(strsplit(x,'"'))[2]} else {x})
    return(cred)
  }
}
