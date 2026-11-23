# https://github.com/apache/arrow/issues/38804
# The reporter's R reprex from the first comment on the issue: the same data
# frame written with three option sets, then read back. Their code is kept;
# added: version prints, tryCatch around each write/read so one failure does
# not stop the rest, and a RESULT line. A correct write gives 3 rows.
options(warn = 1)
library(arrow, warn.conflicts = FALSE)

cat("arrow", as.character(packageVersion("arrow")), "|", R.version.string, "|",
    Sys.info()[["sysname"]], Sys.info()[["release"]], "\n")

tbl_input <- tibble::tibble(
   var_1 = c("arrow", "arrow", "arrow"),
   var_2 = c("arrow", "arrow", "arroW")
)

run_variant <- function(label, compression, use_dictionary) {
  f_dataset_merged <- tempfile()
  out <- tryCatch({
    arrow::write_dataset(
       dataset = tbl_input,
       path = f_dataset_merged,
       partitioning = "var_2",
       compression = compression,
       use_dictionary = use_dictionary
    )
    cat(label, "- directories written:",
        paste(list.files(f_dataset_merged), collapse = ", "), "\n")

    tbl_written <- arrow::open_dataset(
       sources = f_dataset_merged,
       partitioning = arrow::hive_partition()
    )

    res <- tbl_written |>
       dplyr::collect()
    print(res)
    paste0(nrow(res), " rows")
  }, error = function(e) {
    cat(label, "- ERROR:", conditionMessage(e), "\n")
    "ERROR"
  })
  paste0(label, ": ", out)
}

results <- c(
  run_variant("uncompressed+dictionary", "uncompressed", TRUE),
  run_variant("uncompressed+no-dictionary", "uncompressed", FALSE),
  run_variant("snappy+no-dictionary", "snappy", FALSE)
)

cat("RESULT:", paste(results, collapse = "; "), "(expected 3 rows each)\n")
