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

# Usage in Nuvolos applications

Working with R as a Nuvolos application is even simpler:

```
con <- nuvolos::get_connection()
```
