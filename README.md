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
connection_info <- datahub::getDBPath()
con <- datahub::getDataHubCon(connection_info$dbname, connection_info$schemaname)
```
