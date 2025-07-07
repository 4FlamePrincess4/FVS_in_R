library(tidyverse)
library(RSQLite)
library(patchwork)
library(purrr)
library(scales)

setwd("D:/WFSETP/Scenario_Development/Chris_FVS_Runs_KCP_Effects/trt_kcp_effects_summaries_v3")

fvs_output_path <- "D:/WFSETP/FVS_Training/Run_FVS/no_treat_01Jul25_1119/outputs/no_treat_01Jul25_1119.db"
con <- dbConnect(SQLite(), fvs_output_path)

tables <- dbListTables(con)

#####STEP 1: Let's see what we have to work with

#Check the errors
error_table <- dbReadTable(con, "FVS_Error")
unique(error_table$Message)

#I can use this table to subset the outputs by KCPs (KeywordFile)
scenarios <- dbReadTable(con, "FVS_Cases")
view(scenarios)

#This table gives fuel loadings in different pools
fuels_table <- dbReadTable(con, "FVS_Fuels")
head(fuels_table)

#From this, aboveground live and merchantable live might be useful
# Also total removed carbon and carbon released from fire
carb_table <- dbReadTable(con, "FVS_Carbon")
head(carb_table)

#This table might be useful for canopy height & canopy bulk density
canprofile_table <- dbReadTable(con, "FVS_CanProfile")
head(canprofile_table)

#This could be useful for evaluating the FuelTret and FuelMove keywords - 
# downed woody material in different size and hardness classes
down_wood_table <- dbReadTable(con, "FVS_Down_Wood_Cov")
head(down_wood_table)
# 

#This table includes variables describing potential fire behavior
# Also includes canopy base height and canopy density
potfire_table <- dbReadTable(con, "FVS_Potfire")
head(potfire_table)

#I can use this table to summarize removed timber and trees at the stand level
summary_table <- dbReadTable(con, "FVS_Summary")
head(summary_table)

#I can use this table to calculate the species-specific variables
treelist_table <- dbReadTable(con, "FVS_TreeList")
head(treelist_table)

compute_table <- dbReadTable(con, "FVS_Compute")
head(compute_table)

dbDisconnect(con)

###STEP 2: Get vectors of CNs and use these to subset CaseIDs by forest type
all_run_CNs <- scenarios$Stand_CN
#Connect to the FIA database
fia_db_path <- "../../../Data/SQLite_FIADB_ENTIRE/SQLite_FIADB_ENTIRE.db"
conn <- dbConnect(SQLite(), fia_db_path)

#Query REF_FOREST_TYPE table to get VALUE corresponding to the TreeMap CNs
forest_type_lookup <- dbGetQuery(conn, "SELECT PLT_CN, FORTYPCD FROM COND") %>%
  na.omit()  # Remove any NA values
head(forest_type_lookup)

fortyp_char <- as.character(forest_type_lookup$FORTYPCD)
freq(fortyp_char)

#Close the database
dbDisconnect(conn)

#We need the TMFM key table
tmfmkey <- read_csv("D:/WFSETP/Analyses/FuelMap2020_v_2022/TM_ID_20_22.csv")
head(tmfmkey)

#Make a dataframe of the stand CNs
all_run_forest_types <- data.frame(TM_ID_original = as.numeric(all_run_CNs))
head(all_run_forest_types)

#Join the stand CNs with the TMFM key table
all_run_forest_types <- all_run_forest_types %>%
  left_join(tmfmkey, by = "TM_ID_original")
all_run_forest_types$PLT_CN <- as.character(all_run_forest_types$PLT_CN)

#Join the FORTYPCD values with the stand cns
all_run_forest_types <- all_run_forest_types %>%
  left_join(forest_type_lookup, by = "PLT_CN")
head(all_run_forest_types)

#Rename to match the scenario table
all_run_forest_types <- all_run_forest_types %>%
  rename(Stand_CN = TM_ID_original)
head(all_run_forest_types)
all_run_forest_types$Stand_CN <- as.character(all_run_forest_types$Stand_CN)

#Join the FORTYPCD values to the scenario table using the Stand_CN
scenarios <- scenarios %>%
  left_join(all_run_forest_types, by = "Stand_CN") #This forces a many-to-many relationship for some reason, but! The Case_IDs should be unique.
scenarios <- scenarios %>%
  distinct(CaseID, .keep_all = TRUE)
view(scenarios)

#Let's assign the group names to the codes
freq_fortypcd <- as.data.frame(table(scenarios$FORTYPCD))
freq_fortypcd <- freq_fortypcd %>%
  rename(FORTYPCD = Var1) %>%
  mutate(perc = Freq/sum(Freq))
freq_fortypcd <- freq_fortypcd[order(freq_fortypcd$perc, decreasing = TRUE),]
#Read in the names
fortypcd_names <- read_csv("../forypcd_names.csv")
scenarios <- scenarios %>%
  left_join(fortypcd_names, by="FORTYPCD")
head(scenarios)

#Subset the KCP case ID vectors by the 10 most common forest type codes
most_common_FORTYPCDs <- c(185, 201, 221, 281, 901, 184, 266, 267, 268, 265)
focus_scenarios <- scenarios %>%
  dplyr::filter(FORTYPCD %in% most_common_FORTYPCDs)
focus_scenarios <- focus_scenarios %>%
  distinct(CaseID, .keep_all = TRUE)
head(focus_scenarios)
unique(focus_scenarios$FORTYPCD) #It worked! Only the top 10 forest types - 90% of cases

###STEP 3: Get vectors of case numbers for each KCP, across variants
kcp_800_trt <- c("800_fueltret_v2_EC")
kcp_800_move <- c("800_fuelmove_v2_EC")
kcp_802 <- c("802_v2_EC")
kcp_803_move <- c("803_fuelmove_v2_EC")
kcp_803_trt <- c("803_fueltret_v2_EC")
no_treat <- c("no_treatment_EC")

no_treat_caseIDs <- focus_scenarios %>%
  dplyr::filter(KeywordFile %in% no_treat) %>%
  pull(CaseID)
no_treat_caseIDs_df <- data.frame(CaseIDs = no_treat_caseIDs)
write_csv(no_treat_caseIDs_df, "./no_treat_caseIDs.csv")
kcp_800_trt_caseIDs <- focus_scenarios %>%
  dplyr::filter(KeywordFile %in% kcp_800_trt) %>%
  pull(CaseID)
kcp_800_trt_caseIDs_df <- data.frame(CaseIDs = kcp_800_trt_caseIDs)
write_csv(kcp_800_trt_caseIDs_df, "./kcp_800_trt_caseIDs.csv")
kcp_800_move_caseIDs <- focus_scenarios %>%
  dplyr::filter(KeywordFile %in% kcp_800_move) %>%
  pull(CaseID)
kcp_800_move_caseIDs_df <- data.frame(CaseIDs = kcp_800_move_caseIDs)
write_csv(kcp_800_move_caseIDs_df, "./kcp_800_move_caseIDs.csv")
kcp_802_caseIDs <- focus_scenarios %>%
  dplyr::filter(KeywordFile %in% kcp_802) %>%
  pull(CaseID)
kcp_802_caseIDs_df <- data.frame(CaseIDs = kcp_802_caseIDs)
write_csv(kcp_802_caseIDs_df, "./kcp_802_caseIDs.csv")
kcp_803_trt_caseIDs <- focus_scenarios %>%
  dplyr::filter(KeywordFile %in% kcp_803_trt) %>%
  pull(CaseID)
kcp_803_trt_caseIDs_df <- data.frame(CaseIDs = kcp_803_trt_caseIDs)
write_csv(kcp_803_trt_caseIDs_df, "./kcp_803_trt_caseIDs.csv")
kcp_803_move_caseIDs <- focus_scenarios %>%
  dplyr::filter(KeywordFile %in% kcp_803_move) %>%
  pull(CaseID)
kcp_803_move_caseIDss_df <- data.frame(CaseIDs = kcp_803_move_caseIDs)
write_csv(kcp_803_move_caseIDss_df, "./kcp_803_move_caseIDs.csv")

#STEP 4: Now let's summarize forest characteristics on a species level using the tree table
treelist_kcp800_trt <- treelist_table %>%
  dplyr::filter(CaseID %in% kcp_800_trt_caseIDs)
treelist_kcp800_move <- treelist_table %>%
  dplyr::filter(CaseID %in% kcp_800_move_caseIDs)
treelist_kcp802 <- treelist_table %>%
  dplyr::filter(CaseID %in% kcp_802_caseIDs)
treelist_kcp803_trt <- treelist_table %>%
  dplyr::filter(CaseID %in% kcp_803_trt_caseIDs)
treelist_kcp803_move <- treelist_table %>%
  dplyr::filter(CaseID %in% kcp_803_move_caseIDs)

#Add FORTYPCD group name to the table
treelist_kcp800_trt <- treelist_kcp800_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(treelist_kcp800_trt, "./treelist_kcp800_trt.csv")
treelist_kcp800_move <- treelist_kcp800_move %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(treelist_kcp800_move, "./treelist_kcp800_move.csv")
treelist_kcp802 <- treelist_kcp802 %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(treelist_kcp802, "./treelist_kcp802.csv")
treelist_kcp803_trt <- treelist_kcp803_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(treelist_kcp803_trt, "./treelist_kcp803_trt.csv")
treelist_kcp803_move <- treelist_kcp803_move %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(treelist_kcp803_move, "./treelist_kcp803_move.csv")

#Add a KCP label for later plotting
treelist_kcp800_trt <- treelist_kcp800_trt %>%
  mutate(KCP = rep("800_trt", nrow(treelist_kcp800_trt)))%>%
  dplyr::filter(Year != 2022)
treelist_kcp800_move <- treelist_kcp800_move %>%
  mutate(KCP = rep("800_move", nrow(treelist_kcp800_move)))%>%
  dplyr::filter(Year != 2022)
treelist_kcp802 <- treelist_kcp802 %>%
  mutate(KCP = rep("802", nrow(treelist_kcp802)))%>%
  dplyr::filter(Year != 2022)
treelist_kcp803_trt <- treelist_kcp803_trt %>%
  mutate(KCP = rep("803_trt", nrow(treelist_kcp803_trt)))%>%
  dplyr::filter(Year != 2022)
treelist_kcp803_move <- treelist_kcp803_move %>%
  mutate(KCP = rep("803_move", nrow(treelist_kcp803_move)))%>%
  dplyr::filter(Year != 2022)

#Let's create a color palette so that species have a consistent color across data summaries
all_species <- sort(unique(treelist_kcp800_trt$SpeciesFVS))
species_order <- c("WO","CW","BM","VN","AS","DG","RA","PB","GC","PL","OH",
                   "RC","YC","PY","DF","SF","WF","AF","NF","GF",
                   "LP","PP","WB","WP",
                   "WL","LL","WH","MH","WJ","ES", "OS")
species_colors <- c("#94090D","#D90000","#FF2D00","#FA5B0F","#FD7400","#FF8C00","#fea232","#FF007B","#FF577F","#FF7A8A","#FF05D5",
                    "#FFBE00","#FFD34E","#FFDC00","#AAFF00","#68B701","#9DC964","#60831B","#4ECC00","#3B9900",
                    "#1CD100","#128A00","#0C5D00","#357E29","#05FFEE","#07AAB6","#0066FF","#0048FF","#012FA2","#4400FF","#6A08A6", "#000000")
names(species_colors) <- species_order

#-------------TPA and BA summaries----------------
## Calculate basal area and trees per acre by species (Plotting code below)
# Do this only for KCPs 800_trt, 801, 802, and 803_trt

#Let's try automating this plotting process
treatments <- list(treelist_kcp800_trt, treelist_kcp800_move,  
                   treelist_kcp802, treelist_kcp803_trt, treelist_kcp803_move)
#Starting with TPA by species and DBH class
for(kcp in seq_along(treatments)) {
  
  # Pull the dataframe for this iteration
  df <- treatments[[kcp]]
  kcp_id <- df$KCP[[1]]  # If each df contains a KCP column
  
  # Create the summary dataframe
  summary <- df %>%
    mutate(basal_area_ft2 = 0.005454*(DBH^2)*TPA) %>%
    mutate(DBH_class = cut(DBH, breaks = c(-Inf, 2, 8, 25, Inf),
                           labels = c("DBH ≤2 in", "DBH >2–8 in", "DBH >8–25 in", "DBH >25 in"),
                           right=TRUE)) %>%
    select(CaseID, SpeciesFVS, TPA, basal_area_ft2, FORTYPCD, Group_Name, Year, DBH_class, Ht) %>%
    group_by(CaseID, Group_Name, SpeciesFVS, Year, DBH_class) %>%
    summarize(
      TPA_sum = sum(TPA, na.rm = TRUE),
      basal_area_sum = sum(basal_area_ft2, na.rm=TRUE),
      height_mean = mean(Ht, na.rm=TRUE),
      .groups = 'drop'
    ) %>%
    mutate(Year = as.factor(Year))
  
  # Create a list of plots, one per forest type
  tpa_boxplots <- summary %>%
    filter(TPA_sum > 0) %>%
    group_split(Group_Name) %>%
    map(~ {
      group_name <- unique(.x$Group_Name)
      
      ggplot(.x, aes(x = SpeciesFVS, y = TPA_sum, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_grid(Year ~ DBH_class) +
        scale_fill_manual(values = species_colors) +
        scale_color_manual(values = species_colors) +
        labs(
          title = paste0("KCP ", kcp_id, ": Trees per Acre (TPA) - ", group_name, " forest"),
          x = "Species",
          y = "Trees per Acre (TPA)",
          color = "Species"
        ) +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          legend.position = "bottom",
          legend.box = "horizontal",
          legend.text = element_text(size = 8),
          strip.text = element_text(size = 12)
        ) +
        guides(color = "none") +
        coord_cartesian(ylim = c(0, 1000))
    })
  
  # Get group names in correct order
  group_names <- summary %>%
    filter(TPA_sum > 0) %>%
    group_by(Group_Name) %>%
    group_keys() %>%
    pull(Group_Name)
  
  # Make safe file names
  plot_names <- make.names(group_names, unique = TRUE)
  
  # Save each plot
  walk2(tpa_boxplots, plot_names, ~ {
    ggsave(
      filename = paste0(kcp_id, "_TPA_Boxplot_", .y, ".jpg"),
      plot = .x,
      width = 13,
      height = 12,
      dpi = 300
    )
  })
}

#Now let's make these plots for basal area summaries
for(kcp in seq_along(treatments)) {
  
  # Pull the dataframe for this iteration
  df <- treatments[[kcp]]
  kcp_id <- df$KCP[[1]]  # If each df contains a KCP column
  
  # Create the summary dataframe
  summary <- df %>%
    mutate(basal_area_ft2 = 0.005454*(DBH^2)*TPA) %>%
    mutate(DBH_class = cut(DBH, breaks = c(-Inf, 2, 8, 25, Inf),
                           labels = c("DBH ≤2 in", "DBH >2–8 in", "DBH >8–25 in", "DBH >25 in"),
                           right=TRUE)) %>%
    select(CaseID, SpeciesFVS, TPA, basal_area_ft2, FORTYPCD, Group_Name, Year, DBH_class, Ht) %>%
    group_by(CaseID, Group_Name, SpeciesFVS, Year, DBH_class) %>%
    summarize(
      TPA_sum = sum(TPA, na.rm = TRUE),
      basal_area_sum = sum(basal_area_ft2, na.rm=TRUE),
      height_mean = mean(Ht, na.rm=TRUE),
      .groups = 'drop'
    ) %>%
    mutate(Year = as.factor(Year))
  
  # Create a list of plots, one per forest type
  ba_boxplots <- summary %>%
    filter(basal_area_sum > 0) %>%
    group_split(Group_Name) %>%
    map(~ {
      group_name <- unique(.x$Group_Name)
      
      ggplot(.x, aes(x = SpeciesFVS, y = basal_area_sum, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_grid(Year ~ DBH_class) +
        scale_fill_manual(values = species_colors) +
        scale_color_manual(values = species_colors) +
        labs(
          title = paste0("KCP ", kcp_id, ": Basal Area per acre - ", group_name, " forest"),
          x = "Species",
          y = "Basal Area per acre (ft^2)",
          color = "Species"
        ) +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          legend.position = "bottom",
          legend.box = "horizontal",
          legend.text = element_text(size = 8),
          strip.text = element_text(size = 12)
        ) +
        guides(color = "none") +
        coord_cartesian(ylim = c(0, 200))
    })
  
  # Get group names in correct order
  group_names <- summary %>%
    filter(TPA_sum > 0) %>%
    group_by(Group_Name) %>%
    group_keys() %>%
    pull(Group_Name)
  
  # Make safe file names
  plot_names <- make.names(group_names, unique = TRUE)
  
  # Save each plot
  walk2(ba_boxplots, plot_names, ~ {
    ggsave(
      filename = paste0(kcp_id, "_BA_Boxplot_", .y, ".jpg"),
      plot = .x,
      width = 13,
      height = 12,
      dpi = 300
    )
  })
}

#-------------Summarize the change in TPA and BA between the two years-------
for(kcp in seq_along(treatments)) {
  
  df <- treatments[[kcp]]
  kcp_id <- df$KCP[[1]]
  
  # Create summarized data
  summary <- df %>%
    mutate(basal_area_ft2 = 0.005454 * (DBH^2) * TPA,
           DBH_class = cut(DBH, breaks = c(-Inf, 2, 8, 25, Inf),
                           labels = c("DBH ≤2 in", "DBH >2–8 in", "DBH >8–25 in", "DBH >25 in"),
                           right = TRUE)) %>%
    select(CaseID, SpeciesFVS, TPA, basal_area_ft2, FORTYPCD, Group_Name, Year, DBH_class, Ht) %>%
    group_by(CaseID, Group_Name, SpeciesFVS, Year, DBH_class) %>%
    summarize(
      TPA_sum = sum(TPA, na.rm = TRUE),
      .groups = 'drop'
    )
  
  # Pivot wider to get TPA for 2020 and 2021 in same row
  tpa_change <- summary %>%
    filter(Year %in% c(2020, 2021)) %>%
    pivot_wider(
      names_from = Year,
      values_from = TPA_sum,
      names_prefix = "TPA_"
    ) %>%
    mutate(TPA_change = TPA_2021 - TPA_2020) %>%
    filter(!is.na(TPA_change))
  
  # Split by Group_Name to create one plot per forest type
  change_plots <- tpa_change %>%
    group_split(Group_Name) %>%
    map(~ {
      group_name <- unique(.x$Group_Name)
      
      ggplot(.x, aes(x = SpeciesFVS, y = TPA_change, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_wrap(~ DBH_class, ncol = 2) +
        scale_fill_manual(values = species_colors) +
        scale_color_manual(values = species_colors) +
        labs(
          title = paste0("KCP ", kcp_id, ": Δ TPA (2021 - 2020) - ", group_name, " forest"),
          x = "Species",
          y = "Change in Trees per Acre (Δ TPA)"
        ) +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          legend.position = "bottom",
          legend.box = "horizontal",
          legend.text = element_text(size = 8),
          strip.text = element_text(size = 12)
        ) +
        coord_cartesian(ylim = c(-600, 600))
    })
  
  # Save plots
  group_names <- tpa_change %>%
    group_by(Group_Name) %>%
    group_keys() %>%
    pull(Group_Name)
  
  plot_names <- make.names(group_names, unique = TRUE)
  
  walk2(change_plots, plot_names, ~ {
    ggsave(
      filename = paste0(kcp_id, "_TPA_Change_", .y, ".jpg"),
      plot = .x,
      width = 13,
      height = 12,
      dpi = 300
    )
  })
}

#Same for basal area
for(kcp in seq_along(treatments)) {
  
  # Pull the dataframe for this iteration
  df <- treatments[[kcp]]
  kcp_id <- df$KCP[[1]]  # If each df contains a KCP column
  
  # Create summary dataframe
  summary <- df %>%
    mutate(basal_area_ft2 = 0.005454 * (DBH^2) * TPA,
           DBH_class = cut(DBH, breaks = c(-Inf, 2, 8, 25, Inf),
                           labels = c("DBH ≤2 in", "DBH >2–8 in", "DBH >8–25 in", "DBH >25 in"),
                           right = TRUE)) %>%
    select(CaseID, SpeciesFVS, basal_area_ft2, FORTYPCD, Group_Name, Year, DBH_class) %>%
    group_by(CaseID, Group_Name, SpeciesFVS, Year, DBH_class) %>%
    summarize(
      basal_area_sum = sum(basal_area_ft2, na.rm = TRUE),
      .groups = 'drop'
    )
  
  # Pivot wider to get basal area for 2020 and 2021 on the same row
  ba_change <- summary %>%
    filter(Year %in% c(2020, 2021)) %>%
    pivot_wider(
      names_from = Year,
      values_from = basal_area_sum,
      names_prefix = "BA_"
    ) %>%
    mutate(BA_change = BA_2021 - BA_2020) %>%
    filter(!is.na(BA_change))
  
  # Create a list of plots, one per forest type
  ba_change_plots <- ba_change %>%
    group_split(Group_Name) %>%
    map(~ {
      group_name <- unique(.x$Group_Name)
      
      ggplot(.x, aes(x = SpeciesFVS, y = BA_change, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_wrap(~ DBH_class, ncol = 2) +
        scale_fill_manual(values = species_colors) +
        scale_color_manual(values = species_colors) +
        labs(
          title = paste0("KCP ", kcp_id, ": Δ Basal Area (2021 - 2020) - ", group_name, " forest"),
          x = "Species",
          y = "Change in Basal Area per Acre (Δ ft²)"
        ) +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          legend.position = "bottom",
          legend.box = "horizontal",
          legend.text = element_text(size = 8),
          strip.text = element_text(size = 12)
        ) +
        coord_cartesian(ylim = c(-200, 50)) 
    })
  
  # Get group names and safe plot names
  group_names <- ba_change %>%
    group_by(Group_Name) %>%
    group_keys() %>%
    pull(Group_Name)
  
  plot_names <- make.names(group_names, unique = TRUE)
  
  # Save each plot
  walk2(ba_change_plots, plot_names, ~ {
    ggsave(
      filename = paste0(kcp_id, "_BA_Change_", .y, ".jpg"),
      plot = .x,
      width = 13,
      height = 12,
      dpi = 300
    )
  })
}

#Experiment with better labels
#TPA
for(kcp in seq_along(treatments)) {
  
  df <- treatments[[kcp]]
  kcp_id <- df$KCP[[1]]
  
  # Create summarized data
  summary <- df %>%
    mutate(basal_area_ft2 = 0.005454 * (DBH^2) * TPA,
           DBH_class = cut(DBH, breaks = c(-Inf, 2, 8, 25, Inf),
                           labels = c("DBH ≤2 in", "DBH >2–8 in", "DBH >8–25 in", "DBH >25 in"),
                           right = TRUE)) %>%
    select(CaseID, SpeciesFVS, TPA, basal_area_ft2, FORTYPCD, Group_Name, Year, DBH_class, Ht) %>%
    group_by(CaseID, Group_Name, SpeciesFVS, Year, DBH_class) %>%
    summarize(
      TPA_sum = sum(TPA, na.rm = TRUE),
      .groups = 'drop'
    )
  
  # Pivot wider to get TPA for 2020 and 2021 in same row
  tpa_change <- summary %>%
    filter(Year %in% c(2020, 2021)) %>%
    pivot_wider(
      names_from = Year,
      values_from = TPA_sum,
      names_prefix = "TPA_"
    ) %>%
    mutate(TPA_change = TPA_2021 - TPA_2020) %>%
    filter(!is.na(TPA_change))
  
  # Split DBH classes
  small_classes <- c("DBH ≤2 in", "DBH >2–8 in")
  large_classes <- c("DBH >8–25 in", "DBH >25 in")
  
  tpa_boxplots <- tpa_change %>%
    group_split(Group_Name) %>%
    map(~ {
      group_name <- unique(.x$Group_Name)
      
      # Split data
      small_df <- filter(.x, DBH_class %in% small_classes)
      large_df <- filter(.x, DBH_class %in% large_classes)
      
      # Top plot: small DBH, no x-axis labels
      p1 <- ggplot(small_df, aes(x = SpeciesFVS, y = TPA_change, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_wrap(~ DBH_class, ncol = 2) +
        scale_fill_manual(values = species_colors) +
        labs(title = NULL, x = NULL, y = "Δ TPA") +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          legend.position = "none",
          strip.text = element_text(size = 12),
          plot.margin = margin(0, 10, -5, 10)
        ) +
        coord_cartesian(ylim = c(-600, 600))
      
      # Bottom plot: large DBH, with x-axis labels
      p2 <- ggplot(large_df, aes(x = SpeciesFVS, y = TPA_change, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_wrap(~ DBH_class, ncol = 2) +
        scale_fill_manual(values = species_colors) +
        labs(
          title = paste0("KCP ", kcp_id, ": Δ TPA (2021 - 2020) - ", group_name, " forest"),
          x = "Species",
          y = "Δ Trees per Acre"
        ) +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_text(angle = 45, hjust = 1, size = 8),
          axis.ticks.x = element_line(),
          legend.position = "bottom",
          legend.box = "horizontal",
          legend.text = element_text(size = 8),
          strip.text = element_text(size = 12),
          plot.margin = margin(0, 10, 10, 10)
        ) +
        coord_cartesian(ylim = c(-600, 600))
      
      # Combine plots
      wrap_plots(p1, p2, ncol = 1, heights = c(1, 1.1))
    })
  
  
  # Save plots
  group_names <- tpa_change %>%
    group_by(Group_Name) %>%
    group_keys() %>%
    pull(Group_Name)
  
  plot_names <- make.names(group_names, unique = TRUE)
  
  walk2(tpa_boxplots, plot_names, ~ {
    ggsave(
      filename = paste0(kcp_id, "_TPA_Change2_", .y, ".jpg"),
      plot = .x,
      width = 13,
      height = 12,
      dpi = 300
    )
  })
}

#Basal Area
for(kcp in seq_along(treatments)) {
  
  # Pull the dataframe for this iteration
  df <- treatments[[kcp]]
  kcp_id <- df$KCP[[1]]  # If each df contains a KCP column
  
  # Create summary dataframe
  summary <- df %>%
    mutate(basal_area_ft2 = 0.005454 * (DBH^2) * TPA,
           DBH_class = cut(DBH, breaks = c(-Inf, 2, 8, 25, Inf),
                           labels = c("DBH ≤2 in", "DBH >2–8 in", "DBH >8–25 in", "DBH >25 in"),
                           right = TRUE)) %>%
    select(CaseID, SpeciesFVS, basal_area_ft2, FORTYPCD, Group_Name, Year, DBH_class) %>%
    group_by(CaseID, Group_Name, SpeciesFVS, Year, DBH_class) %>%
    summarize(
      basal_area_sum = sum(basal_area_ft2, na.rm = TRUE),
      .groups = 'drop'
    )
  
  # Pivot wider to get basal area for 2020 and 2021 on the same row
  ba_change <- summary %>%
    filter(Year %in% c(2020, 2021)) %>%
    pivot_wider(
      names_from = Year,
      values_from = basal_area_sum,
      names_prefix = "BA_"
    ) %>%
    mutate(BA_change = BA_2021 - BA_2020) %>%
    filter(!is.na(BA_change))
  
  # Split DBH classes into two groups: small and large
  small_classes <- c("DBH ≤2 in", "DBH >2–8 in")
  large_classes <- c("DBH >8–25 in", "DBH >25 in")
  
  ba_change_plots <- ba_change %>%
    group_split(Group_Name) %>%
    map(~ {
      group_name <- unique(.x$Group_Name)
      
      # Split data for facets with and without x-axis labels
      small_df <- filter(.x, DBH_class %in% small_classes)
      large_df <- filter(.x, DBH_class %in% large_classes)
      
      # Plot without x-axis text (small DBH)
      p1 <- ggplot(small_df, aes(x = SpeciesFVS, y = BA_change, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_wrap(~ DBH_class, ncol = 2) +
        scale_fill_manual(values = species_colors) +
        labs(title = NULL, x = NULL, y = "Δ Basal Area (ft²)") +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          legend.position = "none",
          strip.text = element_text(size = 12),
          plot.margin = margin(0, 10, -5, 10)
        ) +
        coord_cartesian(ylim = c(-200, 50))
      
      # Plot with x-axis text (large DBH)
      p2 <- ggplot(large_df, aes(x = SpeciesFVS, y = BA_change, fill = SpeciesFVS)) +
        geom_boxplot(outlier.shape = "diamond", width = 0.6, alpha = 0.7) +
        facet_wrap(~ DBH_class, ncol = 2) +
        scale_fill_manual(values = species_colors) +
        labs(
          title = paste0("KCP ", kcp_id, ": Δ Basal Area (2021 - 2020) - ", group_name, " forest"),
          x = "Species",
          y = "Δ Basal Area (ft²)"
        ) +
        theme_minimal(base_size = 14) +
        theme(
          axis.text.x = element_text(angle = 45, hjust = 1, size = 8),
          axis.ticks.x = element_line(),
          legend.position = "bottom",
          legend.box = "horizontal",
          legend.text = element_text(size = 8),
          strip.text = element_text(size = 12),
          plot.margin = margin(0, 10, 10, 10)
        ) +
        coord_cartesian(ylim = c(-200, 50))
      
      # Combine plots vertically
      patchwork::wrap_plots(p1, p2, ncol = 1, heights = c(1, 1.1))
    })
  
  # Get group names and safe plot names
  group_names <- ba_change %>%
    group_by(Group_Name) %>%
    group_keys() %>%
    pull(Group_Name)
  
  plot_names <- make.names(group_names, unique = TRUE)
  
  # Save each plot
  walk2(ba_change_plots, plot_names, ~ {
    ggsave(
      filename = paste0(kcp_id, "_BA_Change2_", .y, ".jpg"),
      plot = .x,
      width = 13,
      height = 12,
      dpi = 300
    )
  })
}


#-------------Surface fuels summaries----------------
#This version has forest type across the top and year down the side
plot_hist_with_mean <- function(data, variable, xlab) {
  ggplot(data, aes_string(x = variable)) +
    geom_histogram(binwidth = NULL, fill = "steelblue", color = "white", alpha = 0.7) +
    geom_vline(data = data %>%
                 group_by(Group_Name, Year) %>%
                 summarize(mean_val = mean(.data[[variable]], na.rm = TRUE), .groups = 'drop'),
               aes(xintercept = mean_val), color = "red", linetype = "dashed", linewidth = 1) +
    facet_grid(Year ~ Group_Name, scales = "free") +
    labs(x = xlab, y = "Count") +
    theme_minimal(base_size = 16)+
    coord_cartesian(ylim = c(0, 250))
}

#Let's look at variables in the fuels table, then we need to select some forest types to compare across KCPs directly
fuels_no_trt <- fuels_table %>%
  dplyr::filter(CaseID %in% no_treat_caseIDs)
fuels_kcp800_trt <- fuels_table %>%
  dplyr::filter(CaseID %in% kcp_800_trt_caseIDs)
fuels_kcp800_move <- fuels_table %>%
  dplyr::filter(CaseID %in% kcp_800_move_caseIDs)
fuels_kcp802 <- fuels_table %>%
  dplyr::filter(CaseID %in% kcp_802_caseIDs)
fuels_kcp803_trt <- fuels_table %>%
  dplyr::filter(CaseID %in% kcp_803_trt_caseIDs)
fuels_kcp803_move <- fuels_table %>%
  dplyr::filter(CaseID %in% kcp_803_move_caseIDs)

#Add FORTYPCD group name to the table
fuels_no_trt <- fuels_no_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(fuels_no_trt, "./fuels_no_trt.csv")
fuels_kcp800_trt <- fuels_kcp800_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(fuels_kcp800_trt, "./fuels_kcp800_trt.csv")
fuels_kcp800_move <- fuels_kcp800_move %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(fuels_kcp800_move, "./fuels_kcp800_move.csv")
fuels_kcp802 <- fuels_kcp802 %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(fuels_kcp802, "./fuels_kcp802.csv")
fuels_kcp803_trt <- fuels_kcp803_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(fuels_kcp803_trt, "./fuels_kcp803_trt.csv")
fuels_kcp803_move <- fuels_kcp803_move %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(fuels_kcp803_move, "./fuels_kcp803_move.csv")

#We want plots that show pre- and post-treatment  only and across KCPs
#So, keep 2020 and 2021 and add a kcp label
fuels_no_trt <- fuels_no_trt %>%
  mutate(KCP = "No_treatment")%>%
  dplyr::filter(Year == c(2020, 2021))
fuels_kcp800_trt <- fuels_kcp800_trt %>%
  mutate(KCP = "800_trt")%>%
  dplyr::filter(Year == c(2020, 2021))
fuels_kcp800_move <- fuels_kcp800_move %>%
  mutate(KCP = "800_move")%>%
  dplyr::filter(Year == c(2020, 2021))
fuels_kcp802 <- fuels_kcp802 %>%
  mutate(KCP = "802")%>%
  dplyr::filter(Year == c(2020, 2021))
fuels_kcp803_trt <- fuels_kcp803_trt %>%
  mutate(KCP = "803_trt")%>%
  dplyr::filter(Year == c(2020, 2021))
fuels_kcp803_move <- fuels_kcp803_move %>%
  mutate(KCP = "803_move")%>%
  dplyr::filter(Year == c(2020, 2021))

#First, overall summaries by forest type and year
plot_hist_with_mean(fuels_no_trt, "Surface_Total", "Total surface fuel (tons/acre)")
plot_hist_with_mean(fuels_kcp800_trt, "Surface_Total", "Total surface fuel (tons/acre)")
plot_hist_with_mean(fuels_kcp800_move, "Surface_Total", "Total surface fuel (tons/acre)")
plot_hist_with_mean(fuels_kcp802, "Surface_Total", "Total surface fuel (tons/acre)")
plot_hist_with_mean(fuels_kcp803_trt, "Surface_Total", "Total surface fuel (tons/acre)")
plot_hist_with_mean(fuels_kcp803_move, "Surface_Total", "Total surface fuel (tons/acre)")

#Concatenate
fuels_all_kcps <- rbind(fuels_no_trt, fuels_kcp800_trt, fuels_kcp800_move,
                        fuels_kcp802, fuels_kcp803_trt, fuels_kcp803_move)
head(fuels_all_kcps)

#Variables to summarize
fuel_vars <- c("Surface_Total", "Surface_lt3", "Surface_ge3", "Surface_3to6", "Surface_6to12", "Surface_ge12",
  "Standing_Snag_lt3", "Standing_Snag_ge3", "Total_Consumed", "Biomass_Removed")

#Pivot to long form
fuels_long <- fuels_all_kcps %>%
  select(StandID, KCP, Year, Group_Name, all_of(fuel_vars)) %>%
  pivot_longer(cols = all_of(fuel_vars), names_to = "Fuel_Type", values_to = "Fuel_Value")
#Pivot to wide format (2020 and 2021 as columns)
fuels_wide <- fuels_long %>%
  pivot_wider(names_from = Year, values_from = Fuel_Value, names_prefix = "Year_")
#Get no-treatment baseline (Year_2020 for no-treatment)
no_treatment_baseline <- fuels_wide %>%
  filter(KCP == "No_treatment") %>%
  select(StandID, Fuel_Type, Pre_treat = Year_2020)
#Join baseline to all rows
fuels_change_summary <- fuels_wide %>%
  left_join(no_treatment_baseline, by = c("StandID", "Fuel_Type")) %>%
  mutate(
    Mech_Effect = Year_2020 - Pre_treat,
    Burn_Effect = Year_2021 - Year_2020
  )
#Plot by looping through fuel types, faceting by forest type and KCP
plot_fuel_change <- function(data, fuel_type, group_name) {
  data_filtered <- data %>%
    filter(Fuel_Type == fuel_type, Group_Name == group_name) %>%
    pivot_longer(
      cols = c(Pre_treat, Year_2020, Year_2021),
      names_to = "Timepoint",
      values_to = "Fuel_Value"
    )
  
  y_min <- floor(min(data_filtered$Fuel_Value, na.rm = TRUE))
  y_max <- ceiling(max(data_filtered$Fuel_Value, na.rm = TRUE))
  
  ggplot(data_filtered, aes(x = Timepoint, y = Fuel_Value, fill = Timepoint)) +
    geom_boxplot(outlier.alpha = 0.2, width = 0.6) +
    facet_wrap(~ KCP, scales = "free") +
    labs(
      title = paste(fuel_type, "Over Time -", group_name),
      x = "",
      y = "Fuel Load (tons/acre)"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "none",
      axis.text.x = element_text(angle = 20, hjust = 1)
    ) +
    coord_cartesian(ylim = c(y_min, y_max))
}
#Loop through and save plots
unique_fuels <- unique(fuels_change_summary$Fuel_Type)
unique_groups <- unique(fuels_change_summary$Group_Name)

for (fuel in unique_fuels) {
  for (g in unique_groups) {
    plot <- plot_fuel_change(fuels_change_summary, fuel, g)
    
    safe_fuel <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", fuel))
    safe_group <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", g))
    
    ggsave(
      filename = paste0("fuel_plot_", safe_fuel, "_", safe_group, ".jpg"),
      plot = plot,
      width = 12, height = 8, dpi = 300
    )
  }
}

#Plot the mechanical and burn treatment effects
fuel_effects_long <- fuels_change_summary %>%
  pivot_longer(
    cols = c(Mech_Effect, Burn_Effect),
    names_to = "Treatment_Effect",
    values_to = "Effect_Value"
  ) %>%
  mutate(Treatment_Effect = factor(Treatment_Effect, levels=c("Mech_Effect", "Burn_Effect")))

#Plot
plot_fuel_effects <- function(data, fuel_type, group_name) {
  data_filtered <- data %>%
    filter(Fuel_Type == fuel_type, Group_Name == group_name)
  
  y_min <- floor(min(data_filtered$Effect_Value, na.rm = TRUE))
  y_max <- ceiling(max(data_filtered$Effect_Value, na.rm = TRUE))
  
  ggplot(data_filtered, aes(x = Treatment_Effect, y = Effect_Value, fill = Treatment_Effect)) +
    geom_boxplot(outlier.alpha = 0.2, width = 0.6) +
    facet_wrap(~ KCP, scales = "free") +
    labs(
      title = paste("Fuel Load Change (vs previous year) -", fuel_type, "-", group_name),
      x = "",
      y = "Change in Fuel Load (tons/acre)"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "none",
      axis.text.x = element_text(angle = 20, hjust = 1)
    ) +
    coord_cartesian(ylim = c(y_min, y_max))
}
unique_fuels <- unique(fuel_effects_long$Fuel_Type)
unique_groups <- unique(fuel_effects_long$Group_Name)

for (fuel in unique_fuels) {
  for (g in unique_groups) {
    plot <- plot_fuel_effects(fuel_effects_long, fuel, g)
    
    safe_fuel <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", fuel))
    safe_group <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", g))
    
    ggsave(
      filename = paste0("fuel_effect_plot_", safe_fuel, "_", safe_group, ".jpg"),
      plot = plot,
      width = 12, height = 8, dpi = 300
    )
  }
}

##----------Let's compare emissions between fueltret and fuelmove----------
# We need Total_Removed_Carbon for the thinning and Carbon_Released_From_Fire for the pile burn

carb_no_trt <- carb_table %>%
  dplyr::filter(CaseID %in% no_treat_caseIDs)
carb_kcp800_trt <- carb_table %>%
  dplyr::filter(CaseID %in% kcp_800_trt_caseIDs)
carb_kcp800_move <- carb_table %>%
  dplyr::filter(CaseID %in% kcp_800_move_caseIDs)
carb_kcp802 <- carb_table %>%
  dplyr::filter(CaseID %in% kcp_802_caseIDs)
carb_kcp803_trt <- carb_table %>%
  dplyr::filter(CaseID %in% kcp_803_trt_caseIDs)
carb_kcp803_move <- carb_table %>%
  dplyr::filter(CaseID %in% kcp_803_move_caseIDs)

#Add FORTYPCD group name to the table
carb_no_trt <- carb_no_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(carb_no_trt, "./carb_no_trt.csv")
carb_kcp800_trt <- carb_kcp800_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(carb_kcp800_trt, "./carb_kcp800_trt.csv")
carb_kcp800_move <- carb_kcp800_move %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(carb_kcp800_move, "./carb_kcp800_move.csv")
carb_kcp802 <- carb_kcp802 %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(carb_kcp802, "./carb_kcp802.csv")
carb_kcp803_trt <- carb_kcp803_trt %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(carb_kcp803_trt, "./carb_kcp803_trt.csv")
carb_kcp803_move <- carb_kcp803_move %>%
  left_join(focus_scenarios %>% select(CaseID, FORTYPCD, Group_Name), by="CaseID")
write_csv(carb_kcp803_move, "./carb_kcp803_move.csv")

#We want plots that show pre- and post-treatment  only and across KCPs
#So, keep 2020 and 2021 and add a kcp label
carb_no_trt <- carb_no_trt %>%
  mutate(KCP = "No_treatment")%>%
  dplyr::filter(Year == c(2020, 2021))
carb_kcp800_trt <- carb_kcp800_trt %>%
  mutate(KCP = "800_trt")%>%
  dplyr::filter(Year == c(2020, 2021))
carb_kcp800_move <- carb_kcp800_move %>%
  mutate(KCP = "800_move")%>%
  dplyr::filter(Year == c(2020, 2021))
carb_kcp802 <- carb_kcp802 %>%
  mutate(KCP = "802")%>%
  dplyr::filter(Year == c(2020, 2021))
carb_kcp803_trt <- carb_kcp803_trt %>%
  mutate(KCP = "803_trt")%>%
  dplyr::filter(Year == c(2020, 2021))
carb_kcp803_move <- carb_kcp803_move %>%
  mutate(KCP = "803_move")%>%
  dplyr::filter(Year == c(2020, 2021))

#Concatenate
carb_all_kcps <- rbind(carb_no_trt, carb_kcp800_trt, carb_kcp800_move,
                        carb_kcp802, carb_kcp803_trt, carb_kcp803_move)
head(carb_all_kcps)

#Variables to summarize
carb_vars <- c("Total_Removed_Carbon", "Carbon_Released_From_Fire")

#Pivot to long form
carb_long <- carb_all_kcps %>%
  select(StandID, KCP, Year, Group_Name, all_of(carb_vars)) %>%
  pivot_longer(cols = all_of(carb_vars), names_to = "Carb_Type", values_to = "Carb_Value")
#Pivot to wide format (2020 and 2021 as columns)
carb_wide <- carb_long %>%
  pivot_wider(names_from = Year, values_from = Carb_Value, names_prefix = "Year_")
#Get no-treatment baseline (Year_2020 for no-treatment)
no_treatment_baseline <- carb_wide %>%
  filter(KCP == "No_treatment") %>%
  select(StandID, Carb_Type, Pre_treat = Year_2020)
#Join baseline to all rows
carb_change_summary <- carb_wide %>%
  left_join(no_treatment_baseline, by = c("StandID", "Carb_Type")) %>%
  mutate(
    Mech_Effect = Year_2020 - Pre_treat,
    Burn_Effect = Year_2021 - Year_2020
  )
#Plot by looping through fuel types, faceting by forest type and KCP
plot_carb_change <- function(data, Carb_Type, group_name) {
  data_filtered <- data %>%
    filter(Carb_Type == Carb_Type, Group_Name == group_name) %>%
    pivot_longer(
      cols = c(Pre_treat, Year_2020, Year_2021),
      names_to = "Timepoint",
      values_to = "Carb_Value"
    )
  
  y_min <- floor(min(data_filtered$Carb_Value, na.rm = TRUE))
  y_max <- ceiling(max(data_filtered$Carb_Value, na.rm = TRUE))
  
  ggplot(data_filtered, aes(x = Timepoint, y = Carb_Value, fill = Timepoint)) +
    geom_boxplot(outlier.alpha = 0.2, width = 0.6) +
    facet_wrap(~ KCP, scales = "free") +
    labs(
      title = paste(Carb_Type, "Over Time -", group_name),
      x = "",
      y = "Carb Load (tons/acre)"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "none",
      axis.text.x = element_text(angle = 20, hjust = 1)
    ) +
    coord_cartesian(ylim = c(y_min, y_max))
}
#Loop through and save plots
unique_carb <- unique(carb_change_summary$Carb_Type)
unique_groups <- unique(carb_change_summary$Group_Name)

for (carb in unique_carb) {
  for (g in unique_groups) {
    plot <- plot_carb_change(carb_change_summary, carb, g)
    
    safe_carb <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", carb))
    safe_group <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", g))
    
    ggsave(
      filename = paste0("carb_plot_", safe_carb, "_", safe_group, ".jpg"),
      plot = plot,
      width = 12, height = 8, dpi = 300
    )
  }
}

#Plot the mechanical and burn treatment effects
carb_effects_long <- carb_change_summary %>%
  pivot_longer(
    cols = c(Mech_Effect, Burn_Effect),
    names_to = "Treatment_Effect",
    values_to = "Effect_Value"
  ) %>%
  mutate(Treatment_Effect = factor(Treatment_Effect, levels=c("Mech_Effect", "Burn_Effect")))

#Plot
plot_carb_effects <- function(data, Carb_Type, group_name) {
  data_filtered <- data %>%
    filter(Carb_Type == Carb_Type, Group_Name == group_name)
  
  y_min <- floor(min(data_filtered$Effect_Value, na.rm = TRUE))
  y_max <- ceiling(max(data_filtered$Effect_Value, na.rm = TRUE))
  
  ggplot(data_filtered, aes(x = Treatment_Effect, y = Effect_Value, fill = Treatment_Effect)) +
    geom_boxplot(outlier.alpha = 0.2, width = 0.6) +
    facet_wrap(~ KCP, scales = "free") +
    labs(
      title = paste("Carbon Load Change (vs previous year) -", Carb_Type, "-", group_name),
      x = "",
      y = "Change in Carb Load (tons/acre)"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "none",
      axis.text.x = element_text(angle = 20, hjust = 1)
    ) +
    coord_cartesian(ylim = c(y_min, y_max))
}
unique_carb <- unique(carb_effects_long$Carb_Type)
unique_groups <- unique(carb_effects_long$Group_Name)

for (carb in unique_carb) {
  for (g in unique_groups) {
    plot <- plot_carb_effects(carb_effects_long, carb, g)
    
    safe_carb <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", carb))
    safe_group <- gsub("[^A-Za-z0-9_]", "", gsub(" ", "_", g))
    
    ggsave(
      filename = paste0("carb_effect_plot_", safe_carb, "_", safe_group, ".jpg"),
      plot = plot,
      width = 12, height = 8, dpi = 300
    )
  }
}
