# https://github.com/apache/arrow/issues/33486
# CONSTRUCTED: the issue has no code, only screenshots. The reporter says a
# batch of renames (temp file names -> target file names) fails when
# `open_dataset()` is run on the directory WHILE the renames are happening.
# The earlier script (GH-33486.R) only renamed after `open_dataset()` had
# returned. This one covers:
#   A. renames in a second process while this one loops over open_dataset()
#      (with a control: the same renames with nothing reading)
#   B-D. the reprexes from the tickets linked on the thread as "basically
#      this": #31796 (ARROW-16421), #32315 (ARROW-17002), and the 11.0.0.3
#      report on #31796. The `to_duckdb()` one (#32497) is not run.
options(warn = 1)
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

cat("arrow", as.character(packageVersion("arrow")), "|", R.version.string, "|",
    Sys.info()[["sysname"]], Sys.info()[["release"]], "\n")

base <- normalizePath(tempfile(), winslash = "/", mustWork = FALSE)
dir.create(base)
n_files <- 20
n_rounds <- 40

# ---- A. renames in another process while this one reads --------------------
renamer_code <- '
args <- commandArgs(trailingOnly = TRUE)
dir <- args[1]; staging <- args[2]; src <- args[3]
n_files <- as.integer(args[4]); n_rounds <- as.integer(args[5]); out <- args[6]
attempts <- 0L; failures <- 0L; first_failure <- ""
file.create(file.path(dirname(out), "renamer-started"))
for (round in seq_len(n_rounds)) {
  for (i in seq_len(n_files) - 1L) {
    tmp <- file.path(staging, sprintf("part-%d.parquet.tmp", i))
    target <- file.path(dir, sprintf("part-%d.parquet", i))
    file.copy(src, tmp, overwrite = TRUE)
    attempts <- attempts + 1L
    ok <- suppressWarnings(file.rename(tmp, target))
    if (!ok) {
      failures <- failures + 1L
      if (first_failure == "") first_failure <- sprintf("round %d file %d", round, i)
      unlink(tmp)
    }
  }
}
writeLines(c(attempts, failures, first_failure), out)
'
renamer_script <- file.path(base, "renamer.R")
writeLines(renamer_code, renamer_script)
rscript <- file.path(R.home("bin"), "Rscript")

run_renames <- function(label, reader) {
  work <- file.path(base, label)
  dir <- file.path(work, "ds")
  staging <- file.path(work, "staging")
  dir.create(dir, recursive = TRUE)
  dir.create(staging)
  for (i in seq_len(n_files) - 1L) {
    write_parquet(data.frame(x = runif(20000)), file.path(dir, sprintf("part-%d.parquet", i)))
  }
  src <- file.path(work, "replacement.parquet")
  write_parquet(data.frame(x = runif(20000)), src)
  out <- file.path(work, "renamer-result.txt")
  system2(rscript, c("--vanilla", shQuote(renamer_script), shQuote(dir), shQuote(staging),
                     shQuote(src), n_files, n_rounds, shQuote(out)), wait = FALSE)
  started <- file.path(work, "renamer-started")
  t0 <- Sys.time()
  while (!file.exists(started) && difftime(Sys.time(), t0, units = "secs") < 60) Sys.sleep(0.05)
  reads <- 0L; read_errors <- 0L; first_read_error <- ""
  while (!file.exists(out) && difftime(Sys.time(), t0, units = "secs") < 300) {
    res <- reader(dir)
    if (!is.null(res)) {
      reads <- reads + 1L
      if (inherits(res, "error")) {
        read_errors <- read_errors + 1L
        if (first_read_error == "") first_read_error <- conditionMessage(res)
      }
    }
  }
  if (!file.exists(out)) {
    cat(label, ": renamer did not finish within 300s\n")
    return(list(attempts = NA, failures = NA))
  }
  r <- readLines(out)
  cat(sprintf("%s: %s renames attempted, %s FAILED%s; reads in this process: %d, read errors: %d%s\n",
              label, r[1], r[2], if (nzchar(r[3])) paste0(" (first at ", r[3], ")") else "",
              reads, read_errors,
              if (nzchar(first_read_error)) paste0(" (first: ", substr(first_read_error, 1, 200), ")") else ""))
  list(attempts = as.integer(r[1]), failures = as.integer(r[2]))
}

control <- run_renames("A0-control-no-reader", function(dir) { Sys.sleep(0.05); NULL })
open_only <- run_renames("A1-open_dataset-only", function(dir) {
  tryCatch({ ds <- open_dataset(dir); TRUE }, error = function(e) e)
})
open_collect <- run_renames("A2-open_dataset-and-collect", function(dir) {
  tryCatch({ df <- collect(open_dataset(dir)); TRUE }, error = function(e) e)
})

# ---- B. #31796: scan to a RecordBatchReader, then delete --------------------
b_dir <- file.path(base, "B")
write_dataset(iris, b_dir)
con <- open_dataset(b_dir)$NewScan()$Finish()$ToRecordBatchReader()
b_file <- file.path(b_dir, "part-0.parquet")
b1 <- suppressWarnings(file.remove(b_file))
cat("B (#31796): file.remove with an unread RecordBatchReader alive:", b1, "\n")
rm(con); invisible(gc())
b2 <- if (b1) NA else suppressWarnings(file.remove(b_file))
cat("B (#31796): file.remove after rm() and gc():", b2, "\n")

# ---- C. #32315: dplyr query, then delete without gc -------------------------
c_dir <- file.path(base, "C")
write_dataset(iris, c_dir)
ds <- open_dataset(c_dir)
n <- ds %>% count() %>% collect()
c1 <- unlink(c_dir, recursive = TRUE) == 0 && !file.exists(c_dir)
cat("C (#32315): unlink after count() %>% collect(), no gc:", c1, "\n")
rm(ds); invisible(gc())

# ---- D. #31796 comment (arrow 11.0.0.3): join + head + collect --------------
d1_dir <- file.path(base, "D1"); d2_dir <- file.path(base, "D2")
write_dataset(expand.grid(A = 1:150, B = 1:10, C = 1:50), d1_dir, partitioning = "A")
write_dataset(data.frame(A = 1:100, D = 1:100), d2_dir, partitioning = "A")
test <- open_dataset(d1_dir) %>%
  inner_join(open_dataset(d2_dir), by = "A") %>%
  head() %>%
  collect()
rm(test); invisible(gc())
files <- c(dir(d1_dir, full.names = TRUE, recursive = TRUE),
           dir(d2_dir, full.names = TRUE, recursive = TRUE))
removed <- suppressWarnings(vapply(files, file.remove, logical(1)))
cat("D (join + head + collect, then rm and gc):", sum(!removed), "of", length(files),
    "files could not be removed\n")

cat("RESULT: concurrent rename failures control=", control$failures, "/", control$attempts,
    ", open_dataset=", open_only$failures, "/", open_only$attempts,
    ", open+collect=", open_collect$failures, "/", open_collect$attempts,
    "; B remove with reader alive=", b1, ", after gc=", b2,
    "; C unlink without gc=", c1,
    "; D files not removable=", sum(!removed), "/", length(files), "\n", sep = "")
