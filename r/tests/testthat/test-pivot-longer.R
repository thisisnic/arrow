library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

skip_if_not_available("acero")
skip_if_not_installed("tidyr")

tbl <- example_data

test_data <- tibble::tibble(
  id = 1:3,
  name = c("Alice", "Bob", "Charlie"),
  height = c(165, 180, 175),
  width = c(30, 35, 32),
  depth = c(20, 25, 28)
)

billboard_sample <- tibble::tibble(
  artist = c("Radiohead", "Nirvana", "Queen"),
  track = c("Creep", "Smells Like Teen Spirit", "Bohemian Rhapsody"),
  wk1 = c(87, 3, 1),
  wk2 = c(82, 2, 1),
  wk3 = c(72, 1, 2),
  wk4 = c(NA, 1, 3)
)

test_that("pivot_longer converts columns int and dbl to name-value pairs", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(c(int, dbl), names_to = "variable", values_to = "value") %>%
      collect(),
    tbl
  )
})

test_that("pivot_longer with negative column selection excludes chr and fct", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(-c(chr, fct), names_to = "variable", values_to = "value") %>%
      collect(),
    tbl
  )
})

test_that("pivot_longer basic functionality with height width depth columns", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = c(height, width, depth),
        names_to = "dimension",
        values_to = "measurement"
      ) %>%
      collect(),
    test_data
  )
})

test_that("pivot_longer with starts_with selects wk columns", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = starts_with("wk"),
        names_to = "week",
        values_to = "rank"
      ) %>%
      collect(),
    billboard_sample
  )
})

test_that("pivot_longer with names_prefix removes wk prefix", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = starts_with("wk"),
        names_to = "week",
        names_prefix = "wk",
        values_to = "rank"
      ) %>%
      collect(),
    billboard_sample
  )
})

test_that("pivot_longer with values_drop_na removes NA values", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = starts_with("wk"),
        names_to = "week",
        values_to = "rank",
        values_drop_na = TRUE
      ) %>%
      collect(),
    billboard_sample
  )
})

test_that("pivot_longer drops NA values when values_drop_na is TRUE with example_data", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(c(int, lgl), names_to = "var", values_to = "val", values_drop_na = TRUE) %>%
      collect(),
    tbl
  )
})

test_that("pivot_longer with negative column selection excludes id and name", {
  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = -c(id, name),
        names_to = "dimension",
        values_to = "measurement"
      ) %>%
      collect(),
    test_data
  )
})

test_that("pivot_longer with everything() pivots all columns", {
  simple_data <- tibble::tibble(x = c(1, 3), y = c(2, NA))
  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = everything(),
        names_to = "variable",
        values_to = "value"
      ) %>%
      collect(),
    simple_data
  )
})

test_that("pivot_longer with names_pattern extracts multiple variables temp humidity morning evening", {
  complex_data <- tibble::tibble(
    id = 1:2,
    temp_morning_celsius = c(15, 18),
    temp_evening_celsius = c(25, 22),
    humidity_morning_percent = c(60, 65),
    humidity_evening_percent = c(45, 50)
  )

  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = -id,
        names_to = c("measurement", "time", "unit"),
        names_pattern = "(.*)_(.*)_(.*)",
        values_to = "value"
      ) %>%
      collect(),
    complex_data
  )
})

test_that("pivot_longer with names_sep splits column names on underscore", {
  sep_data <- tibble::tibble(
    id = 1:2,
    temp_morning = c(15, 18),
    temp_evening = c(25, 22),
    humidity_morning = c(60, 65),
    humidity_evening = c(45, 50)
  )

  compare_dplyr_binding(
    .input %>%
      pivot_longer(
        cols = -id,
        names_to = c("measurement", "time"),
        names_sep = "_",
        values_to = "value"
      ) %>%
      collect(),
    sep_data
  )
})

test_that("pivot_longer preserves data types with int dbl chr lgl columns", {
  typed_data <- tibble::tibble(
    id = 1:3,
    int_col = c(10L, 20L, 30L),
    dbl_col = c(1.5, 2.5, 3.5),
    chr_col = c("a", "b", "c"),
    lgl_col = c(TRUE, FALSE, TRUE)
  )

  result <- typed_data %>%
    arrow_table() %>%
    pivot_longer(
      cols = -id,
      names_to = "variable",
      values_to = "value"
    ) %>%
    collect()

  expect_true(is.character(result$variable))
  expect_equal(nrow(result), 12) # 3 rows * 4 columns
})

test_that("pivot_longer works with grouped data maintaining groups", {
  grouped_data <- tibble::tibble(
    group = rep(c("A", "B"), each = 2),
    id = 1:4,
    x = c(1, 2, 3, 4),
    y = c(5, 6, 7, 8)
  )

  compare_dplyr_binding(
    .input %>%
      group_by(group) %>%
      pivot_longer(
        cols = c(x, y),
        names_to = "variable",
        values_to = "value"
      ) %>%
      collect(),
    grouped_data
  )
})

test_that("pivot_longer handles large datasets with 1000 rows and 5 measures", {
  large_data <- tibble::tibble(
    id = 1:1000,
    measure1 = rnorm(1000),
    measure2 = rnorm(1000),
    measure3 = rnorm(1000),
    measure4 = rnorm(1000),
    measure5 = rnorm(1000)
  )

  result <- large_data %>%
    arrow_table() %>%
    pivot_longer(
      cols = starts_with("measure"),
      names_to = "measurement",
      values_to = "value"
    ) %>%
    collect()

  expect_equal(nrow(result), 5000) # 1000 rows * 5 measures
  expect_equal(ncol(result), 3)    # id, measurement, value
})

test_that("pivot_longer with empty cols selection returns error", {
  # Both regular tidyr and Arrow should error with empty column selection
  expect_error(
    tbl[1:3, ] %>%
      pivot_longer(
        cols = character(0),
        names_to = "name",
        values_to = "value"
      ),
    "must select at least one column"
  )

  expect_error(
    tbl[1:3, ] %>%
      arrow_table() %>%
      pivot_longer(
        cols = character(0),
        names_to = "name",
        values_to = "value"
      ) %>%
      collect(),
    "must select at least one column"
  )
})

