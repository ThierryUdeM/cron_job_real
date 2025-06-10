library(AzureStor)
library(readr)
library(dplyr)

storage_account_name <- Sys.getenv("AZURE_STORAGE_ACCOUNT")
container_name <- Sys.getenv("AZURE_CONTAINER_NAME")
access_key <- Sys.getenv("AZURE_ACCESS_KEY")

# Create a blob endpoint
endpoint <- storage_endpoint(
  paste0("https://", storage_account_name, ".blob.core.windows.net"),
  key=access_key
)

# Connect to the container
container <- blob_container(endpoint, container_name)

#2. Télécharger données brutes companies
data <- storage_read_csv(container, 'ecosense.csv')
 

if(nrow(data)>25000){
  blobs <- list_blobs(container)
  existing_files <- blobs$name[grepl(paste0("ecosense", "_\\d+\\.csv$"), blobs$name)]
  latest_num <- ifelse(length(existing_files) == 0, 0, 
                       max(as.numeric(gsub(paste0("ecosense", "_(\\d+)\\.csv"), "\\1", existing_files))))
  
  # Write current data to new csv
  new_csv_name <- paste0("ecosense", "_", latest_num + 1, ".csv")
  write_csv(data, new_csv_name)
  upload_blob(container, new_csv_name, dest = new_csv_name)
  data <- data[tail(nrow(data),1),]
  
  tmp_csv <- paste0("ecosense", ".csv")
  write_csv(data, tmp_csv)
  upload_blob(container, tmp_csv, dest = tmp_csv, type = "AppendBlob")
  
}

blobs <- list_blobs(container)
relevant_csvs <- blobs$name[grepl("^ecosense(_\\d+)?\\.csv$", blobs$name)]

clean_names <- function(df) {
  names(df) <- trimws(names(df))
  names(df) <- gsub("\\s+", " ", names(df))
  names(df) <- iconv(names(df), to = "ASCII//TRANSLIT")
  df
}

all_data <- lapply(relevant_csvs, function(csv) {
  message(sprintf("Reading: %s", csv))
  tryCatch({
    df <- storage_read_csv(container, csv)  # ✅ keep it simple!
    df <- clean_names(df)
    return(df)
  }, error = function(e) {
    warning(sprintf("Failed to read %s: %s", csv, e$message))
    NULL
  })
}) %>%
  Filter(Negate(is.null), .) %>%
  bind_rows() %>%
  na.omit()

all_data <- unique(all_data)

# Save as RDS and upload
saveRDS(all_data, "ecosense.rds")
upload_blob(container, "ecosense.rds", dest = "ecosense.rds")
