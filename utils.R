# install.packages("sparklyr")
# install.packages(c("nycflights13", "Lahman"))

library(sparklyr)
library(dplyr)

# spark_install(version = "2.1.0")

# Connect to a local spark cluster
sc <- spark_connect(master = "local")

iris_tbl <- copy_to(sc, iris)
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")

src_tbls(sc)
