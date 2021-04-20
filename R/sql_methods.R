require(reticulate)

#imoporting python-based nuvolos connector package and pandas
nuvolos <- import("nuvolos")
pd <- import("pandas")

dbGetQuery <- function(sql, con){

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

  # using nuvolos python package's to_sql() method
  return(nuvolos$to_sql(df=dbname, name=name, con=con, database=database, schema=schema,
                 if_exists="fail", index=TRUE, index_label=NULL, nanoseconds=FALSE))
}

dbExecute <- function(sql, con){

  # using python's execute method on the established connection
  return(con$execute(sql))
}


