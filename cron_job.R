library(AzureStor)
library(readr)

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
data_2 <- storage_read_csv(container, 'ecosense_1.csv')
data <- bind_rows(data,data_2)
data <- na.omit(data)
saveRDS(data, "file.rds")

upload_blob(container, "file.rds", dest = "ecosense.rds")
