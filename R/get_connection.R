#' R connector for Nuvolos.cloud
#'
#' Function get_connection(dbname, schemaname)
#' Creates a new connection to schema dbname.schemaname. If both arguments are NULL then get_connection will try to call get_nuvolos_db_path().
#' 
#' @param username The Nuvolos username of the connecting user. To obtain the username, please consult the connection guide on Nuvolos.
#' @param password The Nuvolos password of the connecting user. To obtain the password, please consult the connection guide on Nuvolos.
#' @param dbname The database (organization + space) to connect to
#' @param schemaname The schema (instance + state) to connect to
#' @return Return an ODBC connection object.
#'
#' @examples
#' con <- get_connection()
#' con <- get_connection(dbname = "my_database", schemaname = "my_schema")
#' con <- get_connection(username = "my_user", 
#'                       password = "my_password", 
#'                       dbname = "my_database", schemaname = "my_schema")
#'
#' @export
get_connection <- function(username = NULL, password = NULL, dbname = NULL, schemaname = NULL){
  require(DBI)
  require(keyring)
  
  if (is_local()){
    conn_param = get_local_info(username, password, dbname, schemaname)
  } else {
    conn_param = get_nuvolos_info(username, password, dbname, schemaname)
  }
  
  username = conn_param[['username']]
  password = conn_param[['password']]
  dbname = conn_param[['dbname']]
  schemaname = conn_param[['schemaname']]
  
  sysname  <- Sys.info()["sysname"]
  if (sysname == "Linux") {
    con <- odbc::dbConnect(odbc::odbc(),
                           uid=username,
                           pwd=password,
                           driver="SnowflakeDSIIDriver",
                           server="alphacruncher.eu-central-1.snowflakecomputing.com",
                           database=dbname,
                           schema=schemaname,
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
                           role=username,
                           tracing=0)
  }
  options(odbc.batch_rows = 10000)
  return(con)
}

#' Function get_nuvolos_db_path()
#' In Nuvolos applications returns the database name and schema name.
#' Outside Nuvolos it will return with an error: please refer to the connection guide for detailed information on database and schema names for out-of-Nuvolos usage.
get_nuvolos_db_path <- function() {
  path_filename <- Sys.getenv("ACLIB_DBPATH_FILE", "/lifecycle/.dbpath")
  if (!file.exists(path_filename))
    stop(paste0('Could not find dbpath file ', path_filename, '.'))
  
  con = file(path_filename, "r")
  line = readLines(con, n = 1, warn = FALSE)
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

#' Function input_nuvolos_credential()
#' Using outside of Nuvolos only. This helps the user to store credentials safely at local device.
#' @export
input_nuvolos_credential <- function(){
  # store username & password
  username <- rstudioapi::askForSecret(name = "username", message = "Please input your Nuvolos username:", title = 'Nuvolos username')
  keyring::key_set_with_value("nuvolos", "username", username)
  
  password <- rstudioapi::askForSecret(name = "password", message = "Please input your Nuvolos password:", title = 'Nuvolos password')
  keyring::key_set_with_value("nuvolos", username, password)
}

credd_from_local <- function(){
  # retrieve username & password
  tryCatch({
    username = keyring::key_get("nuvolos", "username")
  }, error = function (){
    username <- rstudioapi::askForSecret(name = "username", message = "Please input your Nuvolos username:", title = 'Nuvolos username')
    keyring::key_set_with_value("nuvolos", "username", username)
  })
  
  tryCatch({
    password = keyring::key_get("nuvolos", username)
  }, error = function (){
    password <- rstudioapi::askForSecret(name = "password", message = "Please input your Nuvolos password:", title = 'Nuvolos password')
    keyring::key_set_with_value("nuvolos", username, password)
  })
  return(list('username'=username, 'password'=password))
}

#' check if the environment is local or in nuvolos
is_local <- function(){
  path_filename <- Sys.getenv("ACLIB_DBPATH_FILE", "/lifecycle/.dbpath")
  local = FALSE
  if (!file.exists(path_filename)){
    local = TRUE
  } 
  return(local)
}

get_local_info <- function(username = NULL, password = NULL, dbname = NULL, schemaname = NULL){
  # Find credentials from local, if not, create one by keyring
  if (is.null(username) && is.null(password)) {
    cred <- tryCatch({
      credd_from_local()
    }, error = function(e) {
      input_nuvolos_credential()
      cred <- credd_from_local()
      return(cred)
    })
    username = cred[['username']]
    password = cred[['password']]
  } else if(is.null(username) && !is.null(password) ) {
    stop("Inconsistent input: username is missing, but password was provided. Please specify a username or leave both arguments as NULL.")
  } else if(!is.null(username) && is.null(password) ) {
    stop("Inconsistent input: password is missing, but username was provided. Please specify a password or leave both arguments as NULL.")
  }
  
  ## DB & SCHEMA ##
  # Only accept dbname and schemaname together. If both are missing resort to relying on get_nuvolos_db_path() to figure it out. 
  if (is.null(dbname) | is.null(schemaname)) {
    stop("Inconsistent input: please provide both dbname and schemaname.")
  }
  
  return(list('username'=username, 'password'=password, 'dbname'=dbname, 'schemaname'=schemaname))
}

get_nuvolos_info <- function(username = NULL, password = NULL, dbname = NULL, schemaname = NULL){
  # Relying on get_nuvolos_db_path() to find db and schema. 
  if (is.null(dbname) && is.null(schemaname)) {
    conn_info <- get_nuvolos_db_path()
    dbname <- conn_info$dbname
    schemaname <- conn_info$schemaname
  } else if(is.null(dbname) && !is.null(schemaname) ) {
    stop("Inconsistent input: dbname is missing. Please provide both dbname and schemaname, or leave both NULL.")
  } else if(!is.null(dbname) && is.null(schemaname) ) {
    stop("Inconsistent input: schemaname is missing. Please provide both dbname and schemaname, or leave both NULL.")
  }
  
  # Using Kube secret files to substitute.
  if (is.null(username) && is.null(password)) {
    
    path_user <- '/secrets/username'
    path_token <- '/secrets/snowflake_access_token'
    
    if (file.exists(path_user) && file.exists(path_token)) {
      con_user <- file(path_user, "r")
      line_user <- readLines(con_user, n = 1, warn = FALSE)
      close(con_user)
      
      if( length(line_user) == 0){
        stop(paste0('Could not parse username file, first line of ', path_user, ' is empty.'))
      }
      username <- line_user
      
      con_pw <- file(path_token, "r")
      line_pw <- readLines(con_pw, n = 1, warn = FALSE)
      close(con_pw)
      
      if (length(line_pw) == 0 ){
        stop(paste0('Could not parse token file, first line of ', path_token ,' is empty.'))
      }
      password <- line_pw
    } else {
      stop("Cannot find correct credential...")
    }
  }
  return(list('username'=username, 'password'=password, 'dbname'=dbname, 'schemaname'=schemaname))
}
