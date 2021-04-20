dbGetQuery <- function(sql, con){
  require(reticulate)
  
  #imoporting python-based nuvolos connector package
  nuvolos <- import("nuvolos")
  pd <- import("pandas")
  
  # using python's pandas.read_sql() method 
  return(pd$read_sql(sql, con))
}

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
  
  #imoporting python-based nuvolos connector package
  nuvolos <- import("nuvolos")
  
  # using nuvolos python package's to_sql() method
  return(nuvolos$to_sql(df=dbname, name=name, con=con, database=database, schema=schema,
                 if_exists="fail", index=TRUE, index_label=NULL, nanoseconds=FALSE))
}

dbExecute <- function(statement, con){
  require(reticulate)
  
  # using python's execute method on the established connecton
  return(con$execute(statement))
}


