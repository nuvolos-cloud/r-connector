library(nuvolos)

# File for testing to_sql and read_sql functions.
# Number of NA-s, number of rows, basic descriptive statistics, appending feature
# are tested.
# before and after uploading the sample dataframe to Nuvolos.
# THIS IS NOT AN AUTOMATED TEST, SHOULD BE RUN BY THE DEVELOPER AFTER EVERY
# CHANGE IN TO_SQL FUNCTION!
# In order to run the tests, first delete the nuvolos username + password from
# keyring and use the CONNECTOR_TEST user.

db <- read_sql("select * from orders limit 5000", dbname="SNOWFLAKE_SAMPLE_DATA", schemaname="TPCH_SF100")

db[20:200,] <- NA

to_sql(db, name="test", dbname="CONNECTOR_TEST", schemaname="R_CONNECTOR", if_exists="replace", index=FALSE)

db2 <- read_sql("select * from test", dbname="CONNECTOR_TEST", schemaname="R_CONNECTOR")

test_that("Number os NA-s matches", {
  expect_equal(which(is.na(db2)), which(is.na(db)))
  }
  )

test_that("Number of rows", {
  expect_equal(nrow(db2), nrow(db))
})

test_that("Descriptive statistics", {
  for (i in 1:ncol(db)){
    if (class(db[,i]) == "integer" | class(db[,i]) == "numeric") {
      expect_equal(mean(db2[,i], na.rm=TRUE), mean(db[,i], na.rm=TRUE))
      expect_equal(sd(db2[,i], na.rm=TRUE), sd(db[,i], na.rm=TRUE))
    }
    expect_equal(max(db2[,i]), max(db[,i]))
    expect_equal(min(db2[,i]), min(db[,i]))
    
  }
})


rows <- tryCatch({
  df <- read_sql("select * from test2", dbname="CONNECTOR_TEST", schemaname="R_CONNECTOR")
  nrow(df)
}, error = function(e){
  0
})

to_sql(db, name="test2", dbname="CONNECTOR_TEST", schemaname="R_CONNECTOR", if_exists="append", index=FALSE)

db3 <- read_sql("select * from test2", dbname="CONNECTOR_TEST", schemaname="R_CONNECTOR")

test_that("Number of rows in appending", {
  expect_equal(nrow(db3), nrow(db)+rows)
})
