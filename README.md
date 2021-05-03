# Installation

```
install.packages("remotes")
remotes::install_github("nuvolos-cloud/r-connector")
```

# Usage on Nuvolos

The package creates an odbc connection object to the Nuvolos database that is specified. Standard usage in Nuvolos applications is as follows:

```
con <- nuvolos::get_connection()
```


# Usage from local computer

The package can be used on local computer without installing odbc driver as well when using the read_sql, to_sql, execute functions.
Working on local machine requires only to specify the target database and schema and the sql statement. For the first time you need to enter your credentials to connect Nuvolos database. To find them, please consult the connection guide of your worksapce on Nuvolos.

Standard usage from local machine is as follows.

Reading tables:
```
db <- read_sql("SELECT * FROM table, dbname = "organization_name/space_name", 
schemaname = "instance_name/snapshot_name)
```

Writing tables:
```
to_sql(df = df, name = "table", dbname = "organization_name/space_name, 
schemaname = "instance_name/snapshot_name, if_exists = 'replace', index = FALSE)
```

Executing general statements:

```
execute("DROP TABLE table", "organization_name/space_name", 
schemaname = "instance_name/snapshot_name"")
```

# Naming tables

In order to avoid quoting the tables, you should use only lowercase letters in table names to make them case insensitive. In case there are uppercase letters or special characters in the tables names, you have to refer them as follows: \\"Table\\".



