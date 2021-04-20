dbGetQuery <- function(sql, con){
  require(reticulate)
  
  #imoporting python connector package
  nuvolos <- import("nuvolos")
  pd <- import("pandas")
  
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
  
  #imoporting python connector package
  nuvolos <- import("nuvolos")
  
  return(nuvolos$to_sql(df=dbname, name=name, con=con, database=database, schema=schema,
                 if_exists="fail", index=TRUE, index_label=NULL, nanoseconds=FALSE))
}

dbExecute <- function(statement, con){
  require(reticulate)
  
  return(con$execute(statement))
}


