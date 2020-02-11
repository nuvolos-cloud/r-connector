#' R connector for Nuvolos.cloud
#'
#' Function get_connection(dbname, schemaname)
#' Creates a new connection to schema dbname.schemaname
#'
#' @param dbname The database (organization + space) to connect to
#' @param schemaname The schema (instance + state) to connect to
#' @export
get_connection <- function(dbname, schemaname) {
  require(DBI)

	path_user <- '/secrets/username'
	path_token <- '/secrets/snowflake_access_token'

  if (file.exists("~/.odbc.ini")) {
	 print("Using odbc.ini file.")
    inifile <- ini::read.ini('~/.odbc.ini')

    username <- inifile$nuvolos$uid
    password <- inifile$nuvolos$pwd
  } else if (file.exists(path_user) && file.exists(path_token) ) {
	print("Using secret files.")
	con_user <- file(path_user, "r")
	line_user <- readLines(con_user, n = 1)
	close(con_user)

	if( length(line_user) == 0)
		stop(paste0('Could not parse username file, first line of ', path_user, ' is empty.'))
	username <- line_user

	con_pw <- file("", "r")
	line_pw <- readLines(con_pw, n = 1)
	close(con_pw)
	
	if (length(line_pw) == 0 )
		stop(paste0('Could not parse token file, first line of ', path_token ,' is empty.'))
	password <- line_pw
  } else {
    username <- rstudioapi::askForSecret("Nuvolos Username:")
    password <- rstudioapi::askForSecret("Nuvolos Token (not your password!):")
  }
  sysname  <- Sys.info()["sysname"]

  if (sysname == "Linux") {
    con <- odbc::dbConnect(odbc::odbc(),
                           uid=username,
                           pwd=password,
                           driver="SnowflakeDSIIDriver",
                           server="alphacruncher.eu-central-1.snowflakecomputing.com",
                           database=dbname,
                           schema=schemaname,
                           warehouse=username,
                           role=username,
                           tracing=0)

  } else if (sysname == "Windows") {
    con <- odbc::dbConnect(odbc::odbc(),
                           uid = username,
                           pwd = password,
                           driver = "SnowflakeDSIIDriver",
                           server = "alphacruncher.eu-central-1.snowflakecomputing.com",
                           database = dbname,
                           schema = schemaname,
                           warehouse = username,
                           role = username,
                           tracing = 0)

  } else if (sysname == "Darwin") {
    con <- odbc::dbConnect(odbc::odbc(),
                           uid=username,
                           pwd=password,
                           driver="/opt/snowflake/snowflakeodbc/lib/universal/libSnowflake.dylib",
                           server="alphacruncher.eu-central-1.snowflakecomputing.com",
                           database=dbname,
                           schema=schemaname,
                           warehouse=username,
                           role=username,
                           tracing=0)
  }
  options(odbc.batch_rows = 10000)
  return(con)
}

#' Function get_nuvolos_db_path()
#' In Nuvolos applications returns the database name and schema name
#' Outside Nuvolos it will return with an error.
#'
#' @export
get_nuvolos_db_path <- function() {
  path_filename <- Sys.getenv("ACLIB_DBPATH_FILE", "/lifecycle/.dbpath")
  if (!file.exists(path_filename))
    stop(paste0('Could not find dbpath file ', path_filename, '.'))

  con = file(path_filename, "r")
  line = readLines(con, n = 1)
  close(con)

  if (length(line) == 0)
    stop(paste0('Could not parse dbpath file, first line of ', path_filename, ' is empty.'))

  split_arr <- unlist(strsplit(line, "\".\""))
  if (length(split_arr) != 2 )
    stop(paste0("Invalid path format in dbpath file ", path_filename, '.'))

  db_name <- paste0(split_arr[1],"\"")
  schema_name <- paste0("\"",split_arr[2])
  sprintf("Found database = %s, schema = %s in dbpath file %s.", db_name, schema_name, path_filename)

  return(list(dbname = db_name, schemaname = schema_name))
}
