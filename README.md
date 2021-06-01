# Installation

```
options(repos = "https://cran.rstudio.com")
install.packages("remotes")
remotes::install_github("nuvolos-cloud/r-connector")
```

# Usage on Nuvolos

The package creates an odbc connection object to the Nuvolos database containing the tables of the current instance. The connection object can be used with the DBI package. Standard usage in Nuvolos applications is as follows:

```
con <- nuvolos::get_connection()
```

The special functions mentioned below can also be used on Nuvolos, without the dbname and schemaname being specified. The advantage of using to_sql on Nuvolos is that it supports bulk loading while the DBI's dbWriteTable not.

# Usage from local computer

The package can be used on local computer without installing odbc driver  when using the read_sql, to_sql, execute functions.
Working on local machine requires to specify the target database and schema and the sql statement. For the first time you need to enter your credentials as well in a pop-up window to connect Nuvolos database. To find them, please consult the connection guide of your worksapce on Nuvolos.

Standard usage from local machine is as follows.

Reading tables:
```
db <- nuvolos::read_sql("SELECT * FROM table, dbname = "organization_name/space_name", 
schemaname = "instance_name/snapshot_name")
```

Writing tables:
```
nuvolos::to_sql(df = df, name = "table", dbname = "organization_name/space_name", 
schemaname = "instance_name/snapshot_name", if_exists = "replace", index = FALSE)
```

Executing general statements:

```
nuvolos::execute("DROP TABLE table", "organization_name/space_name", 
schemaname = "instance_name/snapshot_name")
```

# Naming tables

In order to avoid quoting the tables, you should use only lowercase letters in table names to make them case insensitive. In case there are uppercase letters or special characters in the tables names, you have to refer them as follows: \\"Table\\".

# Further question on usage

For any further questions please consult the functions' description, which can be called by ?nuvolos::execute for example.


