library(tidyverse)
library(terra)
library(tidyterra)
library(foreign)

setwd("C:/WFSETP/Scenario1")

#Read in ranger districts to identify a test area
ranger_dists <- vect("C:/WFSETP/Data/okawen_ranger_districts/okawen_ranger_districts.shp")
wenatchee_dist <- ranger_dists %>%
  dplyr::filter(DISTRICTNA == "Wenatchee River Ranger District")

#Read in 60 km buffer perimeter
okawen_perim <- vect("C:/WFSETP/Data/OkaWen_Colville_combined_FOAs_perimeter/OkaWen_Colville_combined_FOAs_perimeter.shp")

#Read in TMFM StandIDs
#tmfm_rast <- rast("C:/WFSETP/Data/TMFM2020_OkaWenCol_FVSVar/TMFM2020_OkaWenCol_FVSVar.tif")
tmfm_rast <- rast("C:/WFSETP/Data/OkaWen_rasters_resampled_together/okawen_treemap2020_120m.tif")
TMFM_dbf <- read.dbf("C:/WFSETP/Data/TMFM2020_OkaWenCol_FVSVar/TMFM2020_OkaWenCol_FVSVar.tif.vat.dbf")
TMFM_dbf <- TMFM_dbf %>%
  rename(StandID = Key)
#The raster now is already the Key variable
names(tmfm_rast) <- "StandID"

#IReclassify the tmfm raster to variant code based on the StandID
variant_lookup <- TMFM_dbf %>%
  dplyr::select(c(StandID, FVS_Varian))
fvs_variants <- classify(tmfm_rast, rcl = variant_lookup, others = NA)
plot(fvs_variants)

variant_labels <- TMFM_dbf %>%
  distinct(FVS_Varian, FVSVariant)%>%
  rename(VariantCode = FVS_Varian,
         Variant = FVSVariant)

#Read in scenario polygons
scenario1_polys <- vect("../Test_Scenario/scenario1/scenario1.shp")
# Rasterize variant codes to match raster
scenario_rast <- rasterize(scenario1_polys, fvs_variants, field = "trt_code", touches = TRUE)
# Mask so only cells where treemap2020 has values are kept
scenario_rast <- mask(scenario_rast, fvs_variants)
#Assign all 801 codes to 800
scenario_rast[scenario_rast == 801] <- 800
scenario_rast[scenario_rast == 901] <- 900
plot(scenario_rast)
writeRaster(scenario_rast, "./scenario1_raster.tif", overwrite=TRUE)
scenario_rast <- rast("./scenario1_raster.tif")
unique(values(scenario_rast))

#Import five seasons of selected fires from FSim run to update the landscape
selected_seasons <- read_csv("C:/WFSETP/TTN_paper/FSim_Outputs/okawen_five_year_set_deciles_long.csv")
selected_seasons
five_year_dist <- read_csv("C:/WFSETP/TTN_paper/FSim_Outputs/okawen_five_year_set_distribution_long.csv")

#Read in the SeasonFire rasters for each
study_area <- "okawen"
runs <- c("foa1c_r16", "foa2d_r5", "foa3d_r8")
scenarios <- "LF2022_recoff3"
timepoints <- "time2"

#Create a reclassification matrix for the flame length rasters
# Define breakpoints and midpoints
breaks <- c(0, 2, 4, 6, 8, 12, Inf) 
midpoints <- c(1, 3, 5, 7, 10, 20)
# Create reclassification matrix: from, to, becomes
reclass_mtx <- cbind(breaks[-length(breaks)], breaks[-1], midpoints)

#Iterate over deciles
scenario_rasters <- list()
  
for(scen in 1:length(unique(selected_seasons$decile))){
  
  # Initialize list to store merged rasters for each season
  merged_rasters <- list()
  
  this_decile <- unique(selected_seasons$decile)[[scen]]
  
  scen_seasons <- selected_seasons %>%
    dplyr::filter(decile == this_decile) %>%
    dplyr::select(Season) %>%
    dplyr::pull(Season)
  
  # Loop over each season
  for (i in seq_along(scen_seasons)) {
    season <- scen_seasons[i]
    season_rasters <- list()  # temp list for each FOA's raster for this season
    for (run in runs) {
      seasonfire_dir <- file.path("C:/WFSETP/TTN_paper/FSim_Outputs",
                                  paste0(study_area, "_", run, "_", scenarios, "_", timepoints),
                                  paste0("SeasonFires_merged_tifs_", scenarios, "_", timepoints))
      
      seasonfire_tif <- file.path(seasonfire_dir, paste0("Season", season, "_merged_IDs_ADs_FLs.tif"))
      
      if (file.exists(seasonfire_tif)) {
        r <- rast(seasonfire_tif)       # read all 3 layers
        r <- r[[c(1,3)]]                # keep only layers 1 and 3 (IDs and FLs)
        season_rasters[[run]] <- r
        
      } else {
        warning("Missing file: ", seasonfire_tif)
      }
    }
    
    # Merge across FOAs for this season
    merged <- Reduce(terra::merge, season_rasters)
    # Replace first layer values with season index
    merged[[1]] <- ifel(!is.na(merged[[1]]), i, NA)
    # Reclassify the floating point flame lengths as the midpoint of the flame length category
    merged[[2]] <- classify(merged[[2]], rcl = reclass_mtx, include.lowest = TRUE)
    # Store result
    merged_rasters[[as.character(season)]] <- merged
  }
  
  # Convert list to a SpatRaster stack
  seasonfires_stack <- rast(merged_rasters)
  plot(seasonfires_stack)
  
  scenario_rasters <- c(scenario_rasters, seasonfires_stack)
}

#Create the raster sandwich for tracking FVS runs
#Make sure all the layers correspond in extent, crs, origin, and resolution
# wenatchee_dist <- project(wenatchee_dist, crs(fvs_variants))
# fvs_variants <- crop(fvs_variants, wenatchee_dist, mask=TRUE)
# standid_rast <- crop(tmfm_rast, wenatchee_dist, mask=TRUE)
# scenario_rast <- crop(scenario_rast, wenatchee_dist, mask=TRUE)

#Mask all the fire and treatment layers with the StandID or variant layers - only forested plots
scenario_rast <- mask(scenario_rast, tmfm_rast)

for(scenario in seq_along(scenario_rasters)){
  this_stack <- scenario_rasters[[scenario]]
  
  #plot(this_stack)+title("before resampling")
  
  this_stack <- resample(this_stack, tmfm_rast, method="near")
  #origin(this_stack) <- origin(tmfm_rast)
  
  #plot(this_stack) + title("after resampling")
  
  #this_stack <- crop(this_stack, wenatchee_dist, mask=TRUE)
  tmfm_cropped <- crop(tmfm_rast, this_stack)
  print(identical(tmfm_rast, this_stack))
  print(ext(tmfm_rast))
  print(ext(this_stack))
  print(origin(tmfm_rast))
  print(origin(this_stack))
  this_stack <- mask(this_stack, tmfm_cropped)
  
  plot(this_stack)+title("after cropping")
  
  fvs_variants <- crop(fvs_variants, this_stack)
  scenario_rast <- crop(scenario_rast, this_stack)
  raster_sandwich <- c(fvs_variants, tmfm_cropped, scenario_rast, this_stack)
  names(raster_sandwich)[[1]] <- "Variant"
  plot(raster_sandwich)
  
  writeRaster(raster_sandwich, paste0("./raster_sandwich_seasonIDs_",  
                                      unique(selected_seasons$decile)[[scenario]], ".tif"), overwrite=TRUE)
  
  #Create standID lists of all of the unique combinations of events
  names(raster_sandwich) <- c("VariantCode", "StandID", "Treatment",
                              "FireYear1", "Flame1",
                              "FireYear2", "Flame2",
                              "FireYear3", "Flame3",
                              "FireYear4", "Flame4",
                              "FireYear5", "Flame5")
  
  #Convert to a dataframe
  raster_sandwich_df <- as.data.frame(raster_sandwich)
  print(head(raster_sandwich_df))
  
  #Assign the correct text labels for the fvs variants
  raster_sandwich_df <- raster_sandwich_df %>%
    left_join(variant_labels, by = "VariantCode") %>%  # Only join with variant_labels
    mutate(VariantCode = Variant) %>%
    select(-Variant) %>%
    rename(Variant = VariantCode)
  print(raster_sandwich_df)
  
  unique_sequences <- raster_sandwich_df %>%
    #Combine fire year + flame info for all 5 years into a single string
    mutate(FireEvents = paste0("Y1:", FireYear1, "_F:", Flame1, ";",
                               "Y2:", FireYear2, "_F:", Flame2, ";",
                               "Y3:", FireYear3, "_F:", Flame3, ";",
                               "Y4:", FireYear4, "_F:", Flame4, ";",
                               "Y5:", FireYear5, "_F:", Flame5)) %>%
    group_by(Variant, Treatment, FireEvents) %>%
    summarise(StandIDs = list(unique(StandID)), .groups = "drop") %>%
    #Create a descriptive name for each combination
    mutate(Name = paste0("Var", Variant,
                         "_Trt", Treatment,
                         "_", gsub(";", "_", FireEvents)))
  
  #Create a named list of vectors (StandIDs) for each combination
  standid_list <- setNames(unique_sequences$StandIDs, unique_sequences$Name)
  length(standid_list)
  saveRDS(standid_list, paste0("standid_list_", unique(selected_seasons$decile)[[scenario]], ".rds"))
  
  #Lastly, create a new dataframe from this named list of vectors
  # that assigns the name to each StandID in the list
  standid_df <- map_dfr(names(standid_list), function(combo) {
    data.frame(
      StandID = standid_list[[combo]],
      combo_name = combo,
      stringsAsFactors = FALSE
    )
  }) %>%
    dplyr::filter(!is.na(StandID)) 
  standid_df <- standid_df %>%
    mutate(StandID2 = 1:nrow(standid_df))
  nrow(standid_df)
  #Save the pixel tracking dataframe
  write_csv(standid_df, paste0("./pixel_tracking_dataframe_", 
                               unique(selected_seasons$decile)[[scenario]], ".csv"))
  
  gc()
}

# pixel_tracking_df_90th <- read_csv("C:/WFSETP/Test_Scenario/pixel_tracking_dataframe_90th.csv")
# head(pixel_tracking_df_90th)
# 
# pixel_tracking_df_20th <- read_csv("C:/WFSETP/Test_Scenario/pixel_tracking_dataframe_20th.csv")
# head(pixel_tracking_df_20th)
# 
# rast_90th <- rast("C:/WFSETP/Test_Scenario/raster_sandwich_seasonIDs_90th.tif")
# plot(rast_90th)
# 
# rast_20th <- rast("C:/WFSETP/Test_Scenario/raster_sandwich_seasonIDs_20th.tif")
# plot(rast_20th)
# 
# season1122 <- rast_20th[[9]]
# plot(season1122)

#We don't want the below anymore because we're going to use one unified list instead of four cases with separate scripts.

# #Sort the list into four cases (types of FVS scripts)
# classify_case <- function(treatment, fireevents) {
#   has_treatment <- !is.na(treatment) && treatment != 0
#   has_fire <- grepl("\\d+", fireevents)  # at least one fire year
#   
#   if (!has_treatment && !has_fire) return("no_fire_no_trt")
#   if (has_treatment && !has_fire) return("trt_no_fire")
#   if (has_treatment && has_fire)  return("trt_with_fire")
#   if (!has_treatment && has_fire) return("fire_no_trt")
# }
# unique_sequences <- unique_sequences %>%
#   mutate(Case = mapply(classify_case, Treatment, FireEvents))
# 
# #Create named lists by case
# case_lists <- split(setNames(unique_sequences$StandIDs, unique_sequences$Name), unique_sequences$Case)
# 
# 
# #Map case name to R script
# case_scripts <- list(
#   no_fire_no_trt = "run_no_fire_no_trt.R",
#   trt_no_fire    = "run_trt_no_fire.R",
#   trt_with_fire  = "run_trt_with_fire.R",
#   fire_no_trt    = "run_fire_no_trt.R"
# )
# 
# #Iterate over each case and run corresponding script
# for (case in names(case_lists)) {
#   stand_list <- case_lists[[case]]      # list of StandIDs
#   script <- case_scripts[[case]]        # R script for this case
#   
#   message("Running case: ", case)
#   
#   for (combo_name in names(stand_list)) {
#     stands <- stand_list[[combo_name]]  # vector of StandIDs for this combo
#     
#     # You can pass combo_name + stands into the script using source/local()
#     source(script, local = TRUE)
#   }
# }
