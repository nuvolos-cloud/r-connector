source("R/get_connection.R")
require(reticulate)

# installing python-based nuvolos connector, if not installed yet
tryCatch({
  import("nuvolos")
}, error = function(e) {
  if (is_local()){
    py_install("nuvolos", pip =TRUE)
  } else {
    py_install("nuvolos-odbc", pip =TRUE)
  }
})


#' Execute SQL SELECT statements from Nuvolos.cloud
#'
#' Function dbGetQuery(sql, con).
#' Executes SELECT SQL statements in the connected Nuvolos schema. 
#' 
#' @param sql SQL statement to be executed. Make sure to use quotes around table names.
#' @param con pyodbc connection object when using on Nuvolos and an sqlalchemy connection object when using from a local device. The object can be created by get_connection function.
#' @return Returns an R dataframe object.
#'
#' @examples
#' df <- nuvolos::dbGetQuery('SELECT * FROM "table", con = con)
#'
#' @export
dbGetQuery <- function(sql, con){
  require(reticulate)
  
  # importing python nuvolos connector package
  nuvolos <- import('nuvolos')
  # importing python package pandas
  pd <- import("pandas")
  
  # using python's pandas.read_sql() method 
  return(pd$read_sql(sql, con))
}


#' Write tables to Nuvolos.cloud
#'
#' Function dbWriteTable(dbname, name, con, database, schema, if_exists, index, index_label, nanoseconds).
#' Creates table in the connected nuvolos schema from R dataframe. 
#' 
#' @param dbname Name of the R dataframe to be written to a table.
#' @param name The name of the database table. It will only be quoted and case sensitive if it contains keywords or special chars
#' @param con pyodbc connection object when using on Nuvolos and an sqlalchemy connection object when using from a local device. The object can be created by get_connection function.
#' @param database The name of the database to which data will be inserted.
#' @param schema The name of the schema to which data will be inserted.
#' @param if_exists: How to behave if the table already exists. {‘fail’, ‘replace’, ‘append’}, default ‘fail’
#' * fail: Raise a ValueError.
#' * replace: Drop the table before inserting new values.
#' * append: Insert new values to the existing table.
#' @param index bool, default True: Write DataFrame index as a column. Uses index_label as the column name in the table.
#' @param index_label column label for index column(s). If None is given (default) and index is True, then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
#' @param nanoseconds if True, nanosecond timestamps will be used to upload the data. Limits timestamp range from 1677-09-21 00:12:43.145224192 to 2262-04-11 23:47:16.854775807.
#' @return Returns the COPY INTO command's results to verify ingestion in the form of a tuple of whether all chunks were ingested correctly, # of chunks, # of ingested rows, and ingest's output.
#'
#' @examples
#' df <- nuvolos::dbWriteQuery(dbname = db, name = "table", con = con, if_exists = 'replace', index = FALSE)
#'
#' @export
dbWriteTable <- function(dbname,
                         name,
                         con,
                         database=NULL,
                         schema=NULL,
                         if_exists="fail",
                         index=TRUE,
                         index_label=NULL,
                         nanoseconds=FALSE){
  require(reticulate)
  
  # importing python nuvolos connector package
  nuvolos <- import('nuvolos')
  
  # using nuvolos python package's to_sql() method
  return(nuvolos$to_sql(df=dbname, name=name, con=con, database=database, schema=schema,
                 if_exists="fail", index=TRUE, index_label=NULL, nanoseconds=FALSE))
}


#' Execute any SQL statement from Nuvolos.cloud
#'
#' Function dbExecute(sql, con).
#' Executes any SQL statement in the connected Nuvolos schema. 
#' 
#' @param sql SQL statement to be executed. Note that quoting the tables is needed only if the table name is case sensitive (it contains both upper and lowercase letters or special chars).
#' @param con pyodbc connection object when using on Nuvolos and an sqlalchemy connection object when using from a local device. The object can be created by get_connection function.
#' @return Returns the result of python's execute method.
#' 
#' @examples
#' dbExecute("DROP TABLE table", con)
#' dbExecute("DROP TABLE \"Table\"", con)

#' @export
dbExecute <- function(sql, con){
  require(reticulate)

  # importing python nuvolos connector package
  nuvolos <- import('nuvolos')
  
  # using python's execute method on the established connection
  return(con$execute(sql))
}


