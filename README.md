# Installation

```
install.packages("remotes")
remotes::install_github("nuvolos-cloud/r-connector")
```

# Usage

The package creates an odbc connection object to the Nuvolos database that is specified. Standard usage is as follows:

```
con <- nuvolos::get_connection("\"organization_name/space_name\"", "\"instance_name/snapshot_name\"")
```
To figure out the appropriate parameters, please consult the connection guide of your worksapce on Nuvolos.
The package can be used on local computer without installing odbc driver as well when using the read_sql, to_sql, execute functions.

# Usage from local computer

Working on local machine requires only to specify the target database and schema and the sql statement.
Using for the first time credentials are asked for. To find them, please consult the connection guide of your worksapce on Nuvolos as well.

Reading tables:
```
db <- read_sql("SELECT * FROM \"Table\"", dbname = "\"organization_name/space_name\"", 
schemaname = "\"instance_name/snapshot_name\"")
```

Writing tables:
```
to_sql(df = df, name = "table", dbname = "\"organization_name/space_name\"", 
schemaname = "\"instance_name/snapshot_name\"", if_exists = 'replace', index = FALSE)
```

Executing general statements:

```
execute("DROP TABLE \"Table\"", "\"organization_name/space_name\"", 
schemaname = "\"instance_name/snapshot_name\"")
```

# Usage in Nuvolos applications

Working with R as a Nuvolos application is even simpler:

```
con <- nuvolos::get_connection()
```


