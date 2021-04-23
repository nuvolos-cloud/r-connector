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

#' @export
dbExecute <- function(sql, con){
  require(reticulate)

  # importing python nuvolos connector package
  nuvolos <- import('nuvolos')
  
  # using python's execute method on the established connection
  return(con$execute(sql))
}


