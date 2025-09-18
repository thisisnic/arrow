# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

test_that("pivot_wider works with basic case", {
  df <- data.frame(
    id = c(1, 1, 2, 2),
    name = c("height", "width", "height", "width"),
    value = c(10, 20, 15, 25)
  )

  # Test with Arrow table
  result <- df |>
    arrow_table() |>
    tidyr::pivot_wider(names_from = name, values_from = value) |>
    dplyr::collect() |>
    dplyr::arrange(id)

  expected <- data.frame(
    id = c(1, 2),
    height = c(10, 15),
    width = c(20, 25)
  )

  expect_equal(as.data.frame(result), expected)
})

test_that("pivot_wider works with names_prefix", {
  df <- data.frame(
    id = c(1, 1, 2, 2),
    name = c("height", "width", "height", "width"),
    value = c(10, 20, 15, 25)
  )

  result <- df |>
    arrow_table() |>
    tidyr::pivot_wider(names_from = name, values_from = value, names_prefix = "dim_") |>
    dplyr::collect() |>
    dplyr::arrange(id)

  expected <- data.frame(
    id = c(1, 2),
    dim_height = c(10, 15),
    dim_width = c(20, 25)
  )

  expect_equal(as.data.frame(result), expected)
})

test_that("pivot_wider works with values_fill", {
  df <- data.frame(
    id = c(1, 1, 2),
    name = c("height", "width", "height"),
    value = c(10, 20, 15)
  )

  result <- df |>
    arrow_table() |>
    tidyr::pivot_wider(names_from = name, values_from = value, values_fill = 0) |>
    dplyr::collect() |>
    dplyr::arrange(id)

  expected <- data.frame(
    id = c(1, 2),
    height = c(10, 15),
    width = c(20, 0)
  )

  expect_equal(as.data.frame(result), expected)
})

test_that("pivot_wider works with explicit id_cols", {
  df <- data.frame(
    id = c(1, 1, 2, 2),
    group = c("A", "A", "B", "B"),
    name = c("height", "width", "height", "width"),
    value = c(10, 20, 15, 25)
  )

  result <- df |>
    arrow_table() |>
    tidyr::pivot_wider(id_cols = c(id, group), names_from = name, values_from = value) |>
    dplyr::collect() |>
    dplyr::arrange(id)

  expected <- data.frame(
    id = c(1, 2),
    group = c("A", "B"),
    height = c(10, 15),
    width = c(20, 25)
  )

  expect_equal(as.data.frame(result), expected)
})

test_that("pivot_wider_spec works with basic spec", {
  df <- data.frame(
    id = c(1, 1, 2, 2),
    name = c("height", "width", "height", "width"),
    value = c(10, 20, 15, 25)
  )

  spec <- data.frame(
    .name = c("height", "width"),
    .value = c("value", "value"),
    name = c("height", "width"),
    stringsAsFactors = FALSE
  )

  result <- df |>
    arrow_table() |>
    tidyr::pivot_wider_spec(spec = spec) |>
    dplyr::collect() |>
    dplyr::arrange(id)

  expected <- data.frame(
    id = c(1, 2),
    height = c(10, 15),
    width = c(20, 25)
  )

  expect_equal(as.data.frame(result), expected)
})

test_that("pivot_wider gives error for unsupported features", {
  df <- data.frame(
    id = c(1, 1, 2, 2),
    name = c("height", "width", "height", "width"),
    value = c(10, 20, 15, 25)
  )

  # Test various unsupported features
  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider(names_from = name, values_from = value, values_fn = mean),
    "pivot_wider.*values_fn"
  )

  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider(names_from = name, values_from = value, names_glue = "{name}_{value}"),
    "pivot_wider.*names_glue"
  )

  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider(names_from = name, values_from = value, names_sort = TRUE),
    "pivot_wider.*names_sort"
  )
})

test_that("pivot_wider requires names_from and values_from", {
  df <- data.frame(
    id = c(1, 1, 2, 2),
    name = c("height", "width", "height", "width"),
    value = c(10, 20, 15, 25)
  )

  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider(names_from = name),
    "pivot_wider.*requires.*values_from"
  )

  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider(values_from = value),
    "pivot_wider.*requires.*names_from"
  )
})

test_that("pivot_wider_spec validates spec format", {
  df <- data.frame(
    id = c(1, 1, 2, 2),
    name = c("height", "width", "height", "width"),
    value = c(10, 20, 15, 25)
  )

  # Missing .name column
  bad_spec1 <- data.frame(
    .value = c("value", "value"),
    name = c("height", "width")
  )

  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider_spec(spec = bad_spec1),
    "pivot_wider_spec.*\\.name"
  )

  # Missing .value column
  bad_spec2 <- data.frame(
    .name = c("height", "width"),
    name = c("height", "width")
  )

  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider_spec(spec = bad_spec2),
    "pivot_wider_spec.*\\.value"
  )

  # Non-data.frame spec
  expect_error(
    df |> arrow_table() |> tidyr::pivot_wider_spec(spec = "not a data frame"),
    "pivot_wider_spec.*data.frame"
  )
})