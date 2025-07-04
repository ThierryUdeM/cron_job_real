library(AzureStor)
library(readr)
library(dplyr)
library(lubridate)
library(geosphere)
library(dbscan)

#I - Download Data
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
  
  options("azure_storage_progress_bar" = FALSE)  # optional: turn off progress bar
  # First download the file to local temp directory
  download_blob(
    container,
    src = "ecosense.csv",
    dest = "ecosense.csv",  # or tempfile() if you want
    overwrite = TRUE
  )
  
  # Then parse CSV locally (streaming, robust)
  data <- read_csv("ecosense.csv")
   
  
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
      tmp_file <- tempfile(fileext = ".csv")
      download_blob(container, src = csv, dest = tmp_file, overwrite = TRUE)
      
      df <- read_csv(tmp_file)
      df <- clean_names(df)
      
      # Convert latitude column to numeric, suppress warnings for coercion
      lat_col <- "Latitude (DDMM.MMMM)"
      if (lat_col %in% names(df)) {
        df[[lat_col]] <- suppressWarnings(as.numeric(df[[lat_col]]))
      }
      
      # Hardcode Latitude for EcoSense ID 17
      if ("EcoSense ID" %in% names(df) && lat_col %in% names(df)) {
        df[[lat_col]][df$`EcoSense ID` == 17] <- 4529.876
      }
      
      unlink(tmp_file)
      return(df)
    }, error = function(e) {
      warning(sprintf("Failed to read %s: %s", csv, e$message))
      NULL
    })
  }) %>%
    Filter(Negate(is.null), .) %>%
    bind_rows() %>%
     na.omit()
  
  
  data <- unique(all_data)

#II - Filter per client and admin
      
    #Admin
    to_dd <- function(ddmm, dir) {
      deg <- floor(ddmm/100)
      min <- ddmm - deg*100
      dec <- deg + min/60
      ifelse(dir %in% c("S","W"), -dec, dec)
    }
    
    # vector of the original cols you want to keep (in order)
    orig_vars <- c(
      "Temperature (C)", "Humidity (%)", "Pressure (mb)", "Light (Lux)",
      "CO (Ohm)", "NO2 (Ohms)", "NH3 (Ohm)",
      "PM1 (ug/m3)", "PM2.5 (ug/m3)", "PM4 (ug/m3)", "PM10 (ug/m3)",
      "PM0.5 (#/cm3)", "PM1.0 (#/cm3)", "PM2.5 (#/cm3)",
      "PM4.0 (#/cm3)", "PM10.0 (#/cm3)", "Noise (amplitude)",
      "Latitude (DDMM.MMMM)", "Longitude (DDMM.MMMM)", "Altitude (m)"
    )
    data_admin <- data %>%
      mutate(
        `Latitude (DDMM.MMMM)`  = as.numeric(`Latitude (DDMM.MMMM)`),
        `Longitude (DDMM.MMMM)` = as.numeric(`Longitude (DDMM.MMMM)`)
      )
    
    data_admin <- data_admin %>%
      # 1) parse timestamp & compute decimal lat/lon
      mutate(
        timestamp = ymd_hms(`Date/Time`),
        lat       = to_dd(`Latitude (DDMM.MMMM)`,  `Latitude E (N/S)`),
        lon       = to_dd(`Longitude (DDMM.MMMM)`, `Longitude E (W/E)`),
        bin_min   = floor_date(timestamp, "10 minutes")
      ) %>%
      # 2) group by module & minute
      group_by(`EcoSense ID`, bin_min) %>%
      # 3) find cluster center and drop outliers >1 m
      mutate(
        lat0 = median(lat),
        lon0 = median(lon),
        dist = distHaversine(cbind(lon, lat), cbind(lon0, lat0))
      ) %>%
      filter(dist <= 1) %>%
      # 4) summarise: snap Date/Time, average the 20 vars, carry the directions
      summarise(
        `Date/Time`        = bin_min[1],
        across(all_of(orig_vars), ~ mean(.x, na.rm = TRUE)),
        `Latitude E (N/S)`  = first(`Latitude E (N/S)`),
        `Longitude E (W/E)` = first(`Longitude E (W/E)`),
        .groups = "drop"
      ) %>%
      # 5) final column order
      select(
        `Date/Time`,`EcoSense ID`,
        all_of(orig_vars),
        `Latitude E (N/S)`, `Longitude E (W/E)`
      )
    
    data_admin <- data_admin[,c(1:20,23,21,24,22)]
    data_admin <- na.omit(data_admin)
  
    #SPJD
    data_spjd <- data %>% filter(`EcoSense ID` %in% c(9, 17, 18,19,20,13:16))
    data_spjd <- data_spjd %>% filter(`Date/Time`>=as.Date('2025-06-06'))
    
    data_clean <- data_spjd %>% filter(`EcoSense ID` %in% c(9, 17, 18,19,20))
    data_full<- data_spjd %>% filter(`EcoSense ID` %in% c(13:16))
    
    #A. For fixe module - 10 min
      # helper to turn DDMM.MMMM + N/S, W/E into decimal degrees
      to_dd <- function(ddmm, dir) {
        deg <- floor(ddmm/100)
        min <- ddmm - deg*100
        dec <- deg + min/60
        ifelse(dir %in% c("S","W"), -dec, dec)
      }
      
      # vector of the original cols you want to keep (in order)
      orig_vars <- c(
        "Temperature (C)", "Humidity (%)", "Pressure (mb)", "Light (Lux)",
        "CO (Ohm)", "NO2 (Ohms)", "NH3 (Ohm)",
        "PM1 (ug/m3)", "PM2.5 (ug/m3)", "PM4 (ug/m3)", "PM10 (ug/m3)",
        "PM0.5 (#/cm3)", "PM1.0 (#/cm3)", "PM2.5 (#/cm3)",
        "PM4.0 (#/cm3)", "PM10.0 (#/cm3)", "Noise (amplitude)",
        "Latitude (DDMM.MMMM)", "Longitude (DDMM.MMMM)", "Altitude (m)"
      )
      data_clean <- data_clean %>%
        mutate(
          `Latitude (DDMM.MMMM)`  = as.numeric(`Latitude (DDMM.MMMM)`),
          `Longitude (DDMM.MMMM)` = as.numeric(`Longitude (DDMM.MMMM)`)
        )
      
      data_clean <- data_clean %>%
        # 1) parse timestamp & compute decimal lat/lon
        mutate(
          timestamp = ymd_hms(`Date/Time`),
          lat       = to_dd(`Latitude (DDMM.MMMM)`,  `Latitude E (N/S)`),
          lon       = to_dd(`Longitude (DDMM.MMMM)`, `Longitude E (W/E)`),
          bin_min   = floor_date(timestamp, "10 minutes")
        ) %>%
        # 2) group by module & minute
        group_by(`EcoSense ID`, bin_min) %>%
        # 3) find cluster center and drop outliers >1 m
        mutate(
          lat0 = median(lat),
          lon0 = median(lon),
          dist = distHaversine(cbind(lon, lat), cbind(lon0, lat0))
        ) %>%
        filter(dist <= 1) %>%
        # 4) summarise: snap Date/Time, average the 20 vars, carry the directions
        summarise(
          `Date/Time`        = bin_min[1],
          across(all_of(orig_vars), ~ mean(.x, na.rm = TRUE)),
          `Latitude E (N/S)`  = first(`Latitude E (N/S)`),
          `Longitude E (W/E)` = first(`Longitude E (W/E)`),
          .groups = "drop"
        ) %>%
        # 5) final column order
        select(
          `Date/Time`,`EcoSense ID`,
          all_of(orig_vars),
          `Latitude E (N/S)`, `Longitude E (W/E)`
        )
      
      data_clean <- data_clean[,c(1:20,23,21,24,22)]
      data_clean <- na.omit(data_clean)
      
      #B. For mobile module - 2 min
      # helper to turn DDMM.MMMM + N/S, W/E into decimal degrees
      to_dd <- function(ddmm, dir) {
        deg <- floor(ddmm/100)
        min <- ddmm - deg*100
        dec <- deg + min/60
        ifelse(dir %in% c("S","W"), -dec, dec)
      }
      
      # vector of the original cols you want to keep (in order)
      orig_vars <- c(
        "Temperature (C)", "Humidity (%)", "Pressure (mb)", "Light (Lux)",
        "CO (Ohm)", "NO2 (Ohms)", "NH3 (Ohm)",
        "PM1 (ug/m3)", "PM2.5 (ug/m3)", "PM4 (ug/m3)", "PM10 (ug/m3)",
        "PM0.5 (#/cm3)", "PM1.0 (#/cm3)", "PM2.5 (#/cm3)",
        "PM4.0 (#/cm3)", "PM10.0 (#/cm3)", "Noise (amplitude)",
        "Latitude (DDMM.MMMM)", "Longitude (DDMM.MMMM)", "Altitude (m)"
      )
      data_full <- data_full %>%
        mutate(
          `Latitude (DDMM.MMMM)`  = as.numeric(`Latitude (DDMM.MMMM)`),
          `Longitude (DDMM.MMMM)` = as.numeric(`Longitude (DDMM.MMMM)`)
        )
      
      data_full <- data_full %>%
        # 1) parse timestamp & compute decimal lat/lon
        mutate(
          timestamp = ymd_hms(`Date/Time`),
          lat       = to_dd(`Latitude (DDMM.MMMM)`,  `Latitude E (N/S)`),
          lon       = to_dd(`Longitude (DDMM.MMMM)`, `Longitude E (W/E)`),
          bin_min   = floor_date(timestamp, "1 minutes")
        ) %>%
        # 2) group by module & minute
        group_by(`EcoSense ID`, bin_min) %>%
        # 3) find cluster center and drop outliers >1 m
        mutate(
          lat0 = median(lat),
          lon0 = median(lon),
          dist = distHaversine(cbind(lon, lat), cbind(lon0, lat0))
        ) %>%
        filter(dist <= 1) %>%
        # 4) summarise: snap Date/Time, average the 20 vars, carry the directions
        summarise(
          `Date/Time`        = bin_min[1],
          across(all_of(orig_vars), ~ mean(.x, na.rm = TRUE)),
          `Latitude E (N/S)`  = first(`Latitude E (N/S)`),
          `Longitude E (W/E)` = first(`Longitude E (W/E)`),
          .groups = "drop"
        ) %>%
        # 5) final column order
        select(
          `Date/Time`,`EcoSense ID`,
          all_of(orig_vars),
          `Latitude E (N/S)`, `Longitude E (W/E)`
        )
    
    data_full <- data_full[,c(1:20,23,21,24,22)]
    data_full <- na.omit(data_full)
    
    #Bind rows
    data_spjd <- bind_rows(data_clean,data_full)

        
#III - Save as RDS and upload
    #Save for Admin
      saveRDS(data_admin, "ecosense.rds")
      upload_blob(container, "ecosense.rds", dest = "ecosense.rds")
      
    #Save for spjd
    saveRDS(data_spjd, "ecosense_spjd.rds")
    upload_blob(container, "ecosense_spjd.rds", dest = "ecosense_spjd.rds")
