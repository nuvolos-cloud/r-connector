# Installation

```
install.packages("remotes")
remotes::install_github("nuvolos-cloud/r-connector")
```

# Usage

```
con <- nuvolos::get_connection("\"organization_name/space_name\"", "\"instance_name/snapshot_name\"")
```

# Usage in Nuvolos applications

```
connection_info <- nuvolos::get_nuvolos_db_path()
con <- nuvolos::get_connection(connection_info$dbname, connection_info$schemaname)
```
