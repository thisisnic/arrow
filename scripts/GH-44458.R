# https://github.com/apache/arrow/issues/44458
# The reporter's call is `arrow::create_package_with_all_dependencies()`.
# A committer traced the failure to C:\Windows\System32\bash.exe, a WSL
# launcher that is not bash, being the `bash` that R finds.
# This runs the call twice: once with the runner's PATH as it is, and once
# with System32 put first so the launcher is the `bash` that is found (only
# if the launcher exists on this runner). Added: prints of which bash is
# found, tryCatch, RESULT line.
options(warn = 1)
library(arrow, warn.conflicts = FALSE)

cat("arrow", as.character(packageVersion("arrow")), "|", R.version.string, "\n")

stub <- "C:\\Windows\\System32\\bash.exe"
cat("WSL launcher present at", stub, ":", file.exists(stub), "\n")

attempt <- function(label) {
  cat("\n==", label, "\n")
  cat("Sys.which('bash'):", Sys.which("bash"), "\n")
  dest <- tempfile(fileext = ".tar.gz")
  out <- tryCatch({
    arrow::create_package_with_all_dependencies(dest)
    paste0("OK (", round(file.size(dest) / 1e6), " MB)")
  }, error = function(e) {
    cat("ERROR:", conditionMessage(e), "\n")
    paste0("ERROR: ", substr(conditionMessage(e), 1, 60))
  })
  paste0(label, ": ", out)
}

first <- attempt("PATH as on the runner")

if (file.exists(stub)) {
  old_path <- Sys.getenv("PATH")
  Sys.setenv(PATH = paste("C:\\Windows\\System32", old_path, sep = ";"))
  second <- attempt("System32 first on PATH")
  Sys.setenv(PATH = old_path)
} else {
  second <- "System32 first on PATH: not run, no WSL launcher on this runner"
}

cat("RESULT:", first, ";", second, "\n")
