library(future)
library(future.batchtools)
library(stringr)
library(purrr)

# --- Args: Rscript run_fvs_slurm.R standid_list.rds ---
args <- commandArgs(trailingOnly = TRUE)
standid_file <- args[1]
standid_list <- readRDS(standid_file)

# --- Case classifier ---
classify_case <- function(combo_name) {
  has_treatment <- str_detect(combo_name, "Trt\\d+")
  has_fire <- str_detect(combo_name, "Y\\d+:\\d+")
  
  if (!has_treatment && !has_fire) return("no_fire_no_trt")
  if (has_treatment && !has_fire)  return("trt_no_fire")
  if (has_treatment && has_fire)   return("trt_with_fire")
  if (!has_treatment && has_fire)  return("fire_no_trt")
}

# --- Functions for each workflow ---
run_no_fire_no_trt <- function(variant, stands) {
  message("Running no_fire_no_trt for variant ", variant)
  # write .key/.in, run FVS
}

run_trt_no_fire <- function(variant, treatment, stands) {
  message("Running trt_no_fire for variant ", variant, " with Trt ", treatment)
  # write treatment KCP, .key/.in, run FVS
}

run_trt_with_fire <- function(variant, treatment, fire_events, stands) {
  message("Running trt_with_fire for variant ", variant, " with Trt ", treatment, " and fires ", paste(fire_events, collapse = ", "))
  # write fire + treatment KCPs, .key/.in, run FVS
}

run_fire_no_trt <- function(variant, fire_events, stands) {
  message("Running fire_no_trt for variant ", variant, " with fires ", paste(fire_events, collapse = ", "))
  # write fire KCPs, .key/.in, run FVS
}

# --- Configure SLURM backend ---
plan(batchtools_slurm, template = "slurm.tmpl")

# --- Parallel job submission ---
future_map(names(standid_list), function(combo_name) {
  stands <- standid_list[[combo_name]]
  
  # Parse combo name
  variant <- str_match(combo_name, "Variant([^_]+)")[,2]
  treatment <- str_match(combo_name, "Trt(\\d+)")[,2]
  fire_events <- str_extract_all(combo_name, "Y\\d+:\\d+")[[1]]
  
  # Classify case
  case_type <- classify_case(combo_name)
  
  # Dispatch
  if (case_type == "no_fire_no_trt") run_no_fire_no_trt(variant, stands)
  if (case_type == "trt_no_fire")    run_trt_no_fire(variant, treatment, stands)
  if (case_type == "trt_with_fire")  run_trt_with_fire(variant, treatment, fire_events, stands)
  if (case_type == "fire_no_trt")    run_fire_no_trt(variant, fire_events, stands)
})
