# https://therinspark.com/
# is a great resource to get started with spark in R

# install.packages("sparklyr")
# install.packages(c("nycflights13", "Lahman"))

library(sparklyr)
library(dplyr)
library(ggplot2)

# spark_install(version = "2.1.0")

# Connect to a local spark cluster
sc <- spark_connect(master = "local")

iris_tbl <- copy_to(sc, iris)
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")

src_tbls(sc)

# Use regular dplyr verbs
flights_tbl %>% filter(dep_delay == 2)

delay <- flights_tbl %>%
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect

# plot delays
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)

# Compare speeds!
# Might not be a good idea due to the fact that everything is local...
bench::system_time(
  flights_tbl %>%
    group_by(tailnum) %>%
    summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
    filter(count > 20, dist < 2000, !is.na(delay)) %>%
    collect
  )

bench::system_time(
  nycflights13::flights %>%
    group_by(tailnum) %>%
    summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
    filter(count > 20, dist < 2000, !is.na(delay))
)

