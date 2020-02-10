# Installation

```
install.packages("remotes")
remotes::install_github("nuvolos-cloud/r-connector")
```

# Usage

```
con <- datahub::getDataHubCon("DM", "SAMPLE_DB")
```

# Usage in Nuvolos applications

```
connection_info <- datahub::get_nuvolos_db_path()
con <- nuvolos-clouod::get_connection(connection_info$dbname, connection_info$schemaname)
```
