# R One Billion Row Challenge

The src C code was created with ChatGPT support, in less than 1hour.
The solution is inspired by https://github.com/dannyvankooten/1brc/blob/main/7.c . The `parse_float` was needed to overcome limitations of `atof`. The solution is compiled with -O3.
The performance of C based code exported to R is similar to DuckDb.

```r
# devtools::install()
library(onebillionc)

system.time(result <- calculate_stats(path.expand("~/python-1-billion-row-challenge/measurements.txt")))

station_names <- result[[1]]
min_temps <- result[[2]]
mean_temps <- result[[3]]
max_temps <- result[[4]]

# Print the results
for (i in 1:length(station_names)) {
  cat(station_names[i], min_temps[i], mean_temps[i], max_temps[i], "\n")
}
```
