destruct <- function(path) {
  files <- list.files(
    path = path,
    all.files = TRUE,
    full.names = TRUE,
    recursive = TRUE,
    include.dirs = FALSE
  )
  
  success <- file.remove(files)
  
  if (length(files) > 0L && !all(success)) {
    stop("Did not destruct all files.")
  }
  
  directories <- list.files(
    path = path,
    all.files = TRUE,
    full.names = TRUE,
    recursive = TRUE,
    include.dirs = TRUE
  )
  
  # sort to remove subdirectories first
  # to avoid errors when directories are not empty
  directories <- sort(directories, decreasing = TRUE)
  success <- file.remove(directories)
  
  success <- c(file.remove(path), success)
  
  return(invisible(all(success)))
}