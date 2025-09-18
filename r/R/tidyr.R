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

# The following S3 methods are registered on load if tidyr is present

#' Pivot data from long to wide using a spec
#'
#' This is a low level interface to pivoting, inspired by the cdata package,
#' that allows you to describe pivoting with a data frame.
#'
#' @param data A data frame to pivot.
#' @param spec A specification data frame. This is useful for more complex
#'   pivots because it gives you greater control on how metadata stored in the
#'   column names becomes data. It must have the following columns:
#'   * `.name` gives the name of the column that will be created in the
#'     output.
#'   * `.value` gives the name of the column that will be used to fill in
#'     the values of the output.
#'   All other columns in `spec` should be named to match columns in the
#'   long format of the `data` and contain the values that those columns
#'   should have in the rows where the new column specified by `.name` is
#'   populated.
#' @param id_cols <\code{\link[tidyr:tidyr_tidy_select]{tidy-select}}> A set of
#'   columns that uniquely identifies each observation. Defaults to all columns
#'   in `data` except for the columns specified in `spec`.
#' @param values_fill Optionally, a (scalar) value that specifies what each `value`
#'   should be filled in with when missing.
#' @param values_fn Optionally, a function applied to the value in each cell
#'   in the output. You will typically use this when the combination of
#'   `id_cols` and the columns specified by `spec` does not uniquely identify
#'   an observation.
#' @param unused_fn Optionally, a function applied to summarize the values from
#'   the unused columns (i.e. columns not identified by `id_cols` or `spec`).
#' @param ... Additional arguments passed on to methods.
#'
#' @return An Arrow data object of the same type as \code{data}.
#' @export
pivot_wider_spec.arrow_dplyr_query <- function(data,
                                               spec,
                                               id_cols = NULL,
                                               values_fill = NULL,
                                               values_fn = NULL,
                                               unused_fn = NULL,
                                               ...) {
  try_arrow_dplyr({
    # Validate arguments - Arrow implementation has limitations
    if (!is.null(values_fn)) {
      arrow_not_supported("pivot_wider_spec() with custom values_fn")
    }
    if (!is.null(unused_fn)) {
      arrow_not_supported("pivot_wider_spec() with unused_fn")
    }

    out <- as_adq(data)

    # Validate spec
    if (!is.data.frame(spec)) {
      arrow_not_supported("pivot_wider_spec() with non-data.frame spec")
    }
    if (!".name" %in% names(spec)) {
      arrow_not_supported("pivot_wider_spec() spec must have a `.name` column")
    }
    if (!".value" %in% names(spec)) {
      arrow_not_supported("pivot_wider_spec() spec must have a `.value` column")
    }

    # Get columns specified in spec (excluding .name and .value)
    spec_cols <- setdiff(names(spec), c(".name", ".value"))

    # Determine id_cols
    if (missing(id_cols) || is.null(substitute(id_cols))) {
      # Default: all columns in data except those specified in spec
      all_cols <- names(out)
      id_cols_names <- setdiff(all_cols, c(unique(spec$.value), spec_cols))
    } else {
      # Use tidyselect to evaluate id_cols
      schema_names <- set_names(seq_along(names(out)), names(out))
      id_cols_names <- names(tidyselect::eval_select(enquo(id_cols), data = schema_names))
    }

    # Check for duplicate .name values within each .value column
    # This would indicate multiple aggregation functions for the same output column
    name_value_combinations <- paste(spec$.name, spec$.value, sep = "_")
    if (length(name_value_combinations) != length(unique(name_value_combinations))) {
      arrow_not_supported("pivot_wider_spec() with duplicate .name values per .value column")
    }

    # Check if this is a dataset (we need to collect data to determine unique values)
    if (query_on_dataset(out)) {
      arrow_not_supported("pivot_wider_spec() on datasets")
    }

    # Group by id_cols for aggregation
    if (length(id_cols_names) > 0) {
      result <- out %>%
        dplyr::group_by(dplyr::across(dplyr::all_of(id_cols_names)))
    } else {
      result <- out
    }

    # Create summarise expressions for each row in spec
    summary_exprs <- list()

    for (i in seq_len(nrow(spec))) {
      spec_row <- spec[i, ]
      output_col_name <- spec_row$.name
      value_col <- spec_row$.value

      # Build condition to match this spec row
      conditions <- list()
      for (col in spec_cols) {
        spec_val <- spec_row[[col]]
        conditions[[col]] <- rlang::expr(!!rlang::sym(col) == !!spec_val)
      }

      # Combine conditions with & if multiple
      if (length(conditions) == 1) {
        condition_expr <- conditions[[1]]
      } else if (length(conditions) > 1) {
        condition_expr <- Reduce(function(x, y) rlang::expr(!!x & !!y), conditions)
      } else {
        # No conditions, just use the value column
        condition_expr <- TRUE
      }

      # Create the summarise expression
      summary_exprs[[output_col_name]] <- rlang::expr(
        dplyr::first(dplyr::if_else(!!condition_expr,
                                   !!rlang::sym(value_col),
                                   !!if (is.null(values_fill)) NA else values_fill))
      )
    }

    # Apply the aggregation
    result <- do.call(dplyr::summarise, c(list(result), summary_exprs, list(.groups = "drop")))
    result
  })
}

pivot_wider_spec.Dataset <- pivot_wider_spec.ArrowTabular <- pivot_wider_spec.RecordBatchReader <- pivot_wider_spec.arrow_dplyr_query

# Helper function to build wider spec (similar to tidyr's build_wider_spec)
build_wider_spec <- function(data, names_from, values_from, names_prefix = "", names_sep = "_") {
  # Get unique combinations from names_from columns
  if (query_on_dataset(data)) {
    arrow_not_supported("build_wider_spec() on datasets - unique key values cannot be determined without collecting data")
  }

  # Collect unique combinations
  combinations <- data %>%
    dplyr::select(dplyr::all_of(c(names_from, values_from))) %>%
    dplyr::distinct() %>%
    dplyr::collect()

  # Get unique names_from values
  unique_names <- combinations %>%
    dplyr::select(dplyr::all_of(names_from)) %>%
    dplyr::distinct() %>%
    dplyr::pull(1)

  # Build spec dataframe
  spec_rows <- list()
  for (name_val in unique_names) {
    for (value_col in values_from) {
      # Create column name - for multiple values_from columns, include value column in name
      if (length(values_from) > 1) {
        col_name <- if (nzchar(names_prefix)) {
          paste0(names_prefix, value_col, names_sep, name_val)
        } else {
          paste0(value_col, names_sep, name_val)
        }
      } else {
        col_name <- if (nzchar(names_prefix)) {
          paste0(names_prefix, name_val)
        } else {
          as.character(name_val)
        }
      }

      # Create spec row
      spec_row <- data.frame(
        .name = col_name,
        .value = value_col,
        stringsAsFactors = FALSE
      )

      # Add the names_from column value
      spec_row[[names_from]] <- name_val

      spec_rows[[length(spec_rows) + 1]] <- spec_row
    }
  }

  # Combine all spec rows
  do.call(rbind, spec_rows)
}

#' Pivot data from long to wide
#'
#' \code{pivot_wider()} "widens" data, increasing the number of columns and
#' decreasing the number of rows. The inverse transformation is
#' \code{\link[tidyr:pivot_longer]{pivot_longer()}}.
#'
#' Learn more in \code{vignette("pivot")}.
#'
#' @param data A data frame to pivot.
#' @param id_cols <\code{\link[tidyr:tidyr_tidy_select]{tidy-select}}> A set of
#'   columns that uniquely identifies each observation. Defaults to all columns
#'   in \code{data} except for the columns specified by \code{names_from} and
#'   \code{values_from}. Typically used when you have additional variables that
#'   is directly related.
#' @param id_expand Unused. For compatibility with tidyr.
#' @param names_from <\code{\link[tidyr:tidyr_tidy_select]{tidy-select}}> A pair of
#'   arguments describing which column (or columns) to get the name of the
#'   output column (\code{names_from}), and which column (or columns) to get the
#'   cell values from (\code{values_from}).
#'   If \code{values_from} contains multiple values, the value will be added to
#'   the front of the output column.
#' @param names_prefix String added to the start of every variable name. This is
#'   particularly useful if \code{names_from} is a numeric vector and you want to
#'   create syntactic variable names.
#' @param names_sep If \code{names_from} or \code{values_from} contains multiple
#'   variables, this will be used to join their values together into a single string
#'   to use as a column name.
#' @param names_glue Instead of \code{names_sep} and \code{names_prefix}, you can supply
#'   a glue specification that uses the \code{names_from} columns (and special
#'   \code{.value}) to create custom column names.
#' @param names_sort Should the column names be sorted? If \code{FALSE}, the default,
#'   column names are ordered by first appearance.
#' @param names_vary When \code{names_from} identifies a column (or columns) with
#'   multiple unique values, and multiple \code{values_from} columns are provided,
#'   in what order should the resulting column names be combined?
#'
#'   \code{"fastest"} varies \code{names_from} values fastest, resulting in a column
#'   naming scheme of the form: \code{value1_name1, value1_name2, value2_name1, value2_name2}.
#'   This is the default.
#'
#'   \code{"slowest"} varies \code{names_from} values slowest, resulting in a column
#'   naming scheme of the form: \code{value1_name1, value2_name1, value1_name2, value2_name2}.
#' @param names_expand Should the values in the \code{names_from} columns be expanded
#'   by \code{\link[tidyr:expand]{expand()}} before pivoting? This results in more columns,
#'   the output will contain column names corresponding to a complete expansion of all
#'   possible values in \code{names_from}. Implicit factor levels that aren't represented
#'   in the data will become explicit. Additionally, the row values corresponding to the
#'   expanded \code{names_from} will be filled with the \code{values_fill} value.
#' @param names_repair Treatment of problematic column names:
#'   \itemize{
#'   \item \code{"minimal"}: No name repair or checks, beyond basic existence,
#'   \item \code{"unique"}: Make sure names are unique and not empty,
#'   \item \code{"check_unique"}: (default value), no name repair, but check they are
#'     unique,
#'   \item \code{"universal"}: Make the names unique and syntactic
#'   \item a function: apply custom name repair (e.g., \code{.name_repair = make.names}
#'     for names in the style of base R).
#'   \item A purrr-style anonymous function, see \code{\link[rlang:as_function]{rlang::as_function()}}
#'   }
#'
#'   This argument is passed on as \code{repair} to \code{\link[vctrs:vec_as_names]{vctrs::vec_as_names()}}.
#'   See there for more details on these terms and the strategies used
#'   to enforce them.
#' @param values_from <\code{\link[tidyr:tidyr_tidy_select]{tidy-select}}> A pair of
#'   arguments describing which column (or columns) to get the name of the
#'   output column (\code{names_from}), and which column (or columns) to get the
#'   cell values from (\code{values_from}).
#'   If \code{values_from} contains multiple values, the value will be added to
#'   the front of the output column.
#' @param values_fill Optionally, a (scalar) value that specifies what each \code{value}
#'   should be filled in with when missing.
#'
#'   This can be a named list if you want to apply different fill values to
#'   different value columns.
#' @param values_fn Optionally, a function applied to the value in each cell
#'   in the output. You will typically use this when the combination of
#'   \code{id_cols} and \code{names_from} columns does not uniquely identify an
#'   observation.
#'
#'   This can be a named list if you want to apply different aggregations
#'   to different \code{values_from} columns.
#' @param unused_fn Optionally, a function applied to summarize the values from
#'   the unused columns (i.e. columns not identified by \code{id_cols},
#'   \code{names_from}, or \code{values_from}).
#'
#'   The default drops unused columns but you can also use \code{dplyr::first},
#'   \code{dplyr::last}, or \code{length} to instead summarise into a single value.
#'
#'   For functions that return multiple values, you may want to specify the number
#'   of values returned per group using the \code{.size} argument to [summarise()].
#'   Note that when there are multiple values from the unused columns, these columns
#'   will be duplicated for each unused value, resulting in multiple rows for each
#'   row in the original data frame.
#' @param ... Additional arguments passed on to methods.
#'
#' @return An Arrow data object of the same type as \code{data}.
#' Currently, \code{pivot_wider} only supports a single \code{names_from} and
#' \code{values_from} column, and does not support custom \code{values_fn}.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' # Pivot sales data from long to wide format
#' df <- data.frame(
#'   id = c(1, 1, 2, 2),
#'   variable = c("height", "width", "height", "width"),
#'   value = c(10, 20, 15, 25)
#' )
#'
#' df |>
#'   arrow_table() |>
#'   pivot_wider(names_from = variable, values_from = value) |>
#'   collect()
#' }
#' @export
pivot_wider.arrow_dplyr_query <- function(data,
                                          id_cols = NULL,
                                          id_expand = NULL,
                                          names_from = NULL,
                                          names_prefix = "",
                                          names_sep = "_",
                                          names_glue = NULL,
                                          names_sort = FALSE,
                                          names_vary = c("fastest", "slowest"),
                                          names_expand = FALSE,
                                          names_repair = "check_unique",
                                          values_from = NULL,
                                          values_fill = NULL,
                                          values_fn = NULL,
                                          unused_fn = NULL,
                                          ...) {
  try_arrow_dplyr({
    # Validate arguments - Arrow implementation has limitations
    if (!is.null(values_fn)) {
      arrow_not_supported("pivot_wider() with custom values_fn")
    }
    if (!is.null(unused_fn)) {
      arrow_not_supported("pivot_wider() with unused_fn")
    }
    if (!is.null(names_glue)) {
      arrow_not_supported("pivot_wider() with names_glue")
    }
    if (names_sort) {
      arrow_not_supported("pivot_wider() with names_sort = TRUE")
    }
    names_vary <- match.arg(names_vary)
    if (names_vary != "fastest") {
      arrow_not_supported("pivot_wider() with names_vary != 'fastest'")
    }
    if (names_expand) {
      arrow_not_supported("pivot_wider() with names_expand = TRUE")
    }
    if (names_repair != "check_unique") {
      arrow_not_supported("pivot_wider() with custom names_repair")
    }
    if (!is.null(id_expand)) {
      # This parameter is unused in tidyr too, but we acknowledge it
    }
    if (nzchar(names_sep) && names_sep != "_") {
      arrow_not_supported("pivot_wider() with custom names_sep")
    }

    out <- as_adq(data)

    # Check for required arguments
    if (missing(names_from) || is.null(substitute(names_from))) {
      arrow_not_supported("pivot_wider() requires names_from")
    }
    if (missing(values_from) || is.null(substitute(values_from))) {
      arrow_not_supported("pivot_wider() requires values_from")
    }

    # Evaluate column selections using tidyselect
    schema_names <- set_names(seq_along(names(out)), names(out))
    names_from_cols <- names(tidyselect::eval_select(enquo(names_from), data = schema_names))
    values_from_cols <- names(tidyselect::eval_select(enquo(values_from), data = schema_names))

    if (length(names_from_cols) != 1) {
      arrow_not_supported("pivot_wider() with multiple names_from columns")
    }
    if (length(values_from_cols) != 1) {
      arrow_not_supported("pivot_wider() with multiple values_from columns")
    }

    names_from_col <- names_from_cols[[1]]
    values_from_col <- values_from_cols[[1]]

    # Build the specification using build_wider_spec
    spec <- build_wider_spec(
      data = out,
      names_from = names_from_col,
      values_from = values_from_col,
      names_prefix = names_prefix,
      names_sep = names_sep
    )

    # Use pivot_wider_spec to do the actual pivoting
    pivot_wider_spec.arrow_dplyr_query(
      data = out,
      spec = spec,
      id_cols = {{ id_cols }},
      values_fill = values_fill,
      values_fn = values_fn,
      unused_fn = unused_fn
    )
  })
}

pivot_wider.Dataset <- pivot_wider.ArrowTabular <- pivot_wider.RecordBatchReader <- pivot_wider.arrow_dplyr_query