require(reticulate)

#imoporting python-based nuvolos connector package and pandas

#' @export
nuvolos <- import("nuvolos")

#' @export
pd <- import("pandas")

#' @export
dbGetQuery <- function(sql, con){

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

  # using nuvolos python package's to_sql() method
  return(nuvolos$to_sql(df=dbname, name=name, con=con, database=database, schema=schema,
                 if_exists="fail", index=TRUE, index_label=NULL, nanoseconds=FALSE))
}

#' @export
dbExecute <- function(sql, con){

  # using python's execute method on the established connection
  return(con$execute(sql))
}


