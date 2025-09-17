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

# The following S3 methods are registered on load if dplyr is present

pivot_longer.arrow_dplyr_query <- function(data,
                                          cols,
                                          names_to = "name",
                                          values_to = "value",
                                          names_prefix = NULL,
                                          names_sep = NULL,
                                          names_pattern = NULL,
                                          values_drop_na = FALSE,
                                          ...) {
  try_arrow_dplyr({
    out <- as_adq(data)

    # Convert column selection to field references
    cols_enquo <- enquo(cols)
    selected_cols <- eval_select_arrow(cols_enquo, out)

    if (length(selected_cols) == 0) {
      # Match tidyr's behavior - error on empty column selection
      abort("`cols` must select at least one column.")
    }

    # Build pivot_longer options for C++
    pivot_options <- build_pivot_longer_options(
      selected_cols = selected_cols,
      schema = out$.data$schema,
      names_to = names_to,
      values_to = values_to,
      names_prefix = names_prefix,
      names_sep = names_sep,
      names_pattern = names_pattern,
      values_drop_na = values_drop_na
    )

    # Add pivot operation to query - similar to how summarise works
    out$pivot_longer <- pivot_options

    # Force collapse like summarise does - creates execution boundary
    collapse.arrow_dplyr_query(out)
  })
}

# Register methods for other Arrow objects
pivot_longer.Dataset <- pivot_longer.ArrowTabular <- pivot_longer.RecordBatchReader <- pivot_longer.arrow_dplyr_query

#' Build pivot_longer options for C++ execution
#' @noRd
build_pivot_longer_options <- function(selected_cols, schema, names_to, values_to,
                                      names_prefix, names_sep, names_pattern, values_drop_na) {

  # Validate inputs
  if (length(names_to) == 0 || length(values_to) == 0) {
    abort("Both `names_to` and `values_to` must be non-empty")
  }

  # Check if all selected columns have compatible types
  selected_types <- sapply(names(selected_cols), function(name) {
    schema$GetFieldByName(name)$type$ToString()
  })

  # Arrow's pivot_longer requires all measurement columns to have the same type
  # For mixed types, we need to provide a helpful error message
  unique_types <- unique(selected_types)
  if (length(unique_types) > 1) {
    abort(paste0(
      "Arrow's pivot_longer requires all selected columns to have the same data type. ",
      "Found mixed types: ", paste(unique_types, collapse = ", "), ". ",
      "Please cast columns to a common type before pivoting, or select only columns of the same type."
    ))
  }


  # Create row templates from selected columns
  row_templates <- build_row_templates(
    selected_cols = selected_cols,
    schema = schema,
    names_to = names_to,
    names_prefix = names_prefix,
    names_sep = names_sep,
    names_pattern = names_pattern
  )

  # Return structure matching C++ PivotLongerNodeOptions
  list(
    row_templates = row_templates,
    feature_field_names = names_to,
    measurement_field_names = values_to,
    values_drop_na = values_drop_na
  )
}

#' Build row templates for pivot_longer transformation
#' @noRd
build_row_templates <- function(selected_cols, schema, names_to, names_prefix, names_sep, names_pattern) {

  col_names <- names(selected_cols)


  # Apply names transformations
  feature_names <- transform_column_names(
    col_names,
    names_to,
    names_prefix,
    names_sep,
    names_pattern
  )

  # Build templates - each column becomes one template
  row_templates <- vector("list", length(col_names))

  for (i in seq_along(col_names)) {
    col_name <- col_names[i]
    # Use the position directly from selected_cols (convert from 1-based to 0-based)
    col_index <- as.integer(selected_cols[col_name] - 1)

    row_templates[[i]] <- list(
      feature_values = feature_names[[i]],
      measurement_values = list(list(indices = col_index))
    )
  }


  row_templates
}

#' Transform column names according to pivot_longer parameters
#' @noRd
transform_column_names <- function(col_names, names_to, names_prefix, names_sep, names_pattern) {

  # Ensure col_names is character
  transformed_names <- as.character(col_names)

  # Remove prefix if specified
  if (!is.null(names_prefix)) {
    transformed_names <- sub(paste0("^", names_prefix), "", transformed_names)
  }

  # Handle multiple names_to columns
  if (length(names_to) > 1) {
    if (!is.null(names_sep)) {
      # Split on separator
      split_names <- strsplit(transformed_names, names_sep, fixed = TRUE)
      return(split_names)
    } else if (!is.null(names_pattern)) {
      # Extract using regex pattern
      matches <- regmatches(transformed_names, regexec(names_pattern, transformed_names))
      extracted_names <- lapply(matches, function(x) x[-1])  # Remove full match
      return(extracted_names)
    } else {
      abort("When `names_to` has length > 1, must specify `names_sep` or `names_pattern`")
    }
  }

  # Single names_to column - return as list where each element is a single string
  as.list(transformed_names)
}

#' Evaluate column selection for Arrow objects
#' @noRd
eval_select_arrow <- function(cols_enquo, query) {
  # Use the same approach as other Arrow dplyr functions
  sim_df <- as.data.frame(implicit_schema(query))

  # Evaluate selection
  selected <- tidyselect::eval_select(cols_enquo, sim_df)

  # Return the named vector (positions with names)
  selected
}
