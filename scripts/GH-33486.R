# https://github.com/apache/arrow/issues/33486
# CONSTRUCTED: the issue has no code, only screenshots. The reporter describes
# replacing files in a directory by renaming temp files over them, and says
# that fails once `open_dataset()` has been run on the directory. This is a
# guess at that sequence: replace one Parquet file of a two-file dataset by
# renaming a temp file over it, at four points.
options(warn = 1)
library(arrow, warn.conflicts = FALSE)

cat("arrow", as.character(packageVersion("arrow")), "|", R.version.string, "|",
    Sys.info()[["sysname"]], Sys.info()[["release"]], "\n")

base <- tempfile()
dir <- file.path(base, "ds")
staging <- file.path(base, "staging")
dir.create(dir, recursive = TRUE)
dir.create(staging)
target <- file.path(dir, "part-0.parquet")
write_parquet(data.frame(x = 1:3), target)
write_parquet(data.frame(x = 4:6), file.path(dir, "part-1.parquet"))

swap <- function(label) {
  tmp <- file.path(staging, "part-0.parquet.tmp")
  write_parquet(data.frame(x = 7:9), tmp)
  ok <- file.rename(tmp, target)
  cat(label, ": rename over the target", if (ok) "succeeded" else "FAILED", "\n")
  if (!ok) unlink(tmp)
  ok
}

r1 <- swap("before open_dataset()")
ds <- open_dataset(dir)
r2 <- swap("after open_dataset(), before any read")
df <- dplyr::collect(ds)
cat("rows collected:", nrow(df), "\n")
r3 <- swap("after collect()")
rm(ds, df)
invisible(gc())
r4 <- swap("after rm() and gc()")

cat("RESULT: rename succeeded before open_dataset=", r1,
    ", after open_dataset=", r2,
    ", after collect=", r3,
    ", after rm+gc=", r4, "\n", sep = "")
