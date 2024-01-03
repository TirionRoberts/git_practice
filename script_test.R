# 1. Script details ------------------------------------------------------------

# Name of script: OseltamivirAPIQuerySARI
# Description:  Using R to query the NHSBSA open data portal API. Time series 
# of both community and secondary care oseltamivir supply over time. Comparison with SARI Watch flu admissions 
# Created by: Sarah Whittle
# Created on: 7-11-2023
# Latest update by: 
# Latest update on: 
# Update notes: 
# R version: created in 4.3.1

# 2. Load packages -------------------------------------------------------------
# Package to be used
packages <- c(
  "jsonlite",
  "dplyr",    
  "crul",
  "purrr",
  "fable",
  "tsibble",
  "feasts",
  "lubridate",
  "ggplot2",
  "tidyr",
  "grid",
  "readODS",
  "ISOweek"
)

# Install packages if they aren't already
if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
  install.packages(setdiff(packages, rownames(installed.packages())))  
}

# load packages
lapply(packages, library, character.only = TRUE)

# 3. Load SARI Watch data ------------------------------------------------------

urltmp <- "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1167173/Annual-Influenza-Report-Data-File-2022-2023.ods"
sheetname <- "Table_17__SARI_Watch-hosplong" 
tmp <- tempfile()
download.file(urltmp, dest= tmp, mode = "wb")
dftmp <- readODS::read_ods(tmp,sheet = sheetname, col_names = TRUE, skip  = 2)
unlink(dftmp)

## Clean variable names 
dfclean <- dftmp %>%
  janitor::clean_names()


## Pivot to change winter from column name to variable and estimate absolute number of admissions from rate multiplied by England population 
pop = 56536419
dfclean_weekly <- tidyr::pivot_longer(data = dfclean, 
                                      cols = starts_with("x"),
                                      names_to = "Winter",
                                      values_to = "rate",
                                      names_prefix ="x") %>%
  dplyr::mutate(
    start_year = as.numeric(substr(Winter,1,4)),
    end_year = start_year + 1,
    Date = case_when(week_number >= 40 ~ as.Date(paste(start_year, week_number, 1, sep="-"), "%Y-%U-%u"),
                     week_number < 40 ~ as.Date(paste(end_year, week_number, 1, sep="-"), "%Y-%U-%u")),
    month_day  = format(Date, "%b-%d"),
    est_adm = pop*rate/1e5
  ) %>%
  na.omit()    

#sari_adm <- data.table::fread("./Prescription analysis/oseltamivir/sariwatch/SARI_estimated_admissions_flu.csv")
sari_adm <- data.table::fread("./SARI_estimated_admissions_flu.csv")

# convert isoweeks and winters into year and isoweek
sari_weekly <- tidyr::pivot_longer(data = sari_adm, 
                                   cols = starts_with("20"),
                                   names_to = "winter",
                                   values_to = "admissions") %>% 
  mutate(
    start_year = as.numeric(substr(winter,1,4)),
    end_year = start_year + 1,
    weekdate = case_when(isoweek >= 40 ~ ISOweek2date(sprintf("%d-W%02d-%d", start_year, isoweek, 1)),
                         isoweek < 40 ~ ISOweek2date(sprintf("%d-W%02d-%d", end_year, isoweek, 1))),
    weekyear = ISOweek(weekdate)) %>% 
  select(weekyear, weekdate, admissions)

# 4. Define variables ----------------------------------------------------------

# Define the url for the API call
base_endpoint <- "https://opendata.nhsbsa.net/api/3/action/"
package_list_method <- "package_list"     # List of data-sets in the portal
package_show_method <- "package_show?id=" # List all resources of a data-set
action_method <- "datastore_search_sql?"  # SQL action method

# Send API call to get list of data-sets
datasets_response <- jsonlite::fromJSON(paste0(
  base_endpoint, 
  package_list_method
))

datasets_response$result


dataset_id_pca <- "prescription-cost-analysis-pca-monthly-data"
dataset_id_scmd <- "secondary-care-medicines-data-indicative-price"

# 5. Asynchronously extract data from multiple months (PCA) --------------------------

# this is faster than the for loop alternative
#look at the time frame of data
# this is to list the month/years of data available
metadata_repsonse <- jsonlite::fromJSON(paste0(
  base_endpoint, 
  package_show_method,
  dataset_id_pca
))

# Resource names and IDs are kept within the resources table returned from the 
# package_show_method call.
resources_table <- metadata_repsonse$result$resources
#there is a resource that states SCMDV3_YYYYMM

# this should extract all files
resource_name_list <- resources_table$name
# for a subset of the data then specify the conditions. The below selects a year
# resource_name_list <- resources_table$name[grepl("2020", resources_table$name)]

# Filter queries to only extract oseltamivir supply
medication <- "Oseltamivir" 

# Construct the SQL query as a function
async_query <- function(resource_name) {
  paste0(
    "
  SELECT 
      * 
  FROM `", 
    resource_name, "` 
  WHERE 
        1=1 
        AND BNF_CHEMICAL_SUBSTANCE LIKE '%", medication, "%'
  "
  )
}

# Create the API calls
async_api_calls <- lapply(
  X = resource_name_list,
  FUN = function(x) 
    paste0(
      base_endpoint,
      action_method,
      "resource_id=",
      x, 
      "&",
      "sql=",
      URLencode(async_query(x)) # Encode spaces in the url
    )
)

# Use crul::Async to get the results
dd <- crul::Async$new(urls = async_api_calls)
res <- dd$get()

# Check that everything is a success
all(vapply(res, function(z) z$success(), logical(1)))

# Parse the output into a list of dataframes
async_dfs <- lapply(
  X = res, 
  FUN = function(x) {
    
    # Parse the response
    tmp_response <- x$parse("UTF-8")
    
    # Extract the records
    tmp_df <- jsonlite::fromJSON(tmp_response)$result$result$records
  }
)

# columns between dfs have different classes
# set all to characters

func <- function(data){
  output <- data %>%
    mutate(across(everything(), as.character))
}
tidy_df <- map_dfr(async_dfs, ~func(.x))

# Concatenate the results 
async_df_pca <- do.call(dplyr::bind_rows, tidy_df)


# 6. Asynchronously extract data from multiple months (SCMD) --------------------------
metadata_repsonse <- jsonlite::fromJSON(paste0(
  base_endpoint, 
  package_show_method,
  dataset_id_scmd
))

# Resource names and IDs are kept within the resources table returned from the 
# package_show_method call.
resources_table <- metadata_repsonse$result$resources
#there is a resource that states SCMDV3_YYYYMM

# this should extract all files
resource_name_list <- resources_table$name
# for a subset of the data then specify the conditions. The below selects a year
# resource_name_list <- resources_table$name[grepl("2020", resources_table$name)]

# Filter queries to only extract oseltamivir supply
medication <- "Oseltamivir" 

# Construct the SQL query as a function
async_query <- function(resource_name) {
  paste0(
    "
  SELECT 
      * 
  FROM `", 
    resource_name, "` 
  WHERE 
        1=1 
        AND VMP_PRODUCT_NAME LIKE '%", medication, "%'
  "
  )
}

# Create the API calls
async_api_calls <- lapply(
  X = resource_name_list,
  FUN = function(x) 
    paste0(
      base_endpoint,
      action_method,
      "resource_id=",
      x, 
      "&",
      "sql=",
      URLencode(async_query(x)) # Encode spaces in the url
    )
)

# Use crul::Async to get the results
dd <- crul::Async$new(urls = async_api_calls)
res <- dd$get()

# Check that everything is a success
all(vapply(res, function(z) z$success(), logical(1)))

# Parse the output into a list of dataframes
async_dfs <- lapply(
  X = res, 
  FUN = function(x) {
    
    # Parse the response
    tmp_response <- x$parse("UTF-8")
    
    # Extract the records
    tmp_df <- jsonlite::fromJSON(tmp_response)$result$result$records
  }
)

# some of the YEAR_MONTH columns are written differently to others (some are 
# characters YYYY-MM  and some are integers YYYYMM)
my_func <- function(data, my_col){
  my_col <- enexpr(my_col)
  
  output <- data %>% 
    mutate(!!my_col := (gsub("-","",as.character(!!my_col))))
  
}

tidy_df <- map_dfr(async_dfs, ~my_func(.x, YEAR_MONTH))

# Concatenate the results 
async_df_scmd <- do.call(dplyr::bind_rows, tidy_df)

# 7. Devising counts -----------------------------------------------------------
# some quantities of oseltamivir are negative. Guidance says this is where 
# supply made in a previous month has been returned in a subsequent one

# list all volumes of oseltamivir
unique(async_df_scmd$VMP_PRODUCT_NAME)

# create total columns
async_df_scmd <- async_df_scmd %>% 
  mutate(mls = case_when(UNIT_OF_MEASURE_NAME == "ML" ~ TOTAL_QUANITY_IN_VMP_UNIT,
                         UNIT_OF_MEASURE_NAME == "CAPSULE" ~ 0),
         mgs = case_when(UNIT_OF_MEASURE_NAME == "ML" ~ 0,
                         UNIT_OF_MEASURE_NAME == "CAPSULE" ~ TOTAL_QUANITY_IN_VMP_UNIT),
         total = case_when(UNIT_OF_MEASURE_NAME == "ML" ~ TOTAL_QUANITY_IN_VMP_UNIT,
                           UNIT_OF_MEASURE_NAME == "CAPSULE" ~ TOTAL_QUANITY_IN_VMP_UNIT)) 

oseltamivir_summary <- async_df_scmd %>% 
  group_by(YEAR_MONTH) %>% 
  summarise(total_ml = sum(mls),
            total_capsule = sum(mgs),
            courses_ml = total_ml/65,
            courses_capsule = total_capsule/10,
            total_courses = courses_ml + courses_capsule) %>% 
  ungroup()

# create second data set that excludes returns
oseltamivir_noret <- async_df_scmd %>% 
  filter(TOTAL_QUANITY_IN_VMP_UNIT >= 0)

oseltamivir_noreturn <- oseltamivir_noret %>% 
  group_by(YEAR_MONTH) %>% 
  summarise(total_ml = sum(mls),
            total_capsule = sum(mgs),
            courses_ml = total_ml/65,
            courses_capsule = total_capsule/10,
            total_courses = courses_ml + courses_capsule) %>% 
  ungroup()

# convert prescriptions to estimated weekly counts
oseltamivir_summary <- oseltamivir_summary %>% 
  mutate(YEAR_MONTH = as.Date(paste(YEAR_MONTH, "01", sep=""), format = "%Y%m%d"))

oseltamivir_daily <- oseltamivir_summary %>%
  group_by(as.Date(YEAR_MONTH)) %>%
  expand(Date = seq(floor_date(YEAR_MONTH, unit = "month"),
                    ceiling_date(YEAR_MONTH, unit="month")-days(1), by="day"), total_courses) %>%
  as.data.frame() %>% 
  mutate(ISOweek = ISOweek(Date),
         daysinmonth = days_in_month(Date))

# divide total courses by the number of days in a month
oseltamivir_daily <- oseltamivir_daily %>% 
  mutate(dailycourses = total_courses/daysinmonth)

# sum to weekly courses
oseltamivir_weekly <- oseltamivir_daily %>% 
  group_by(ISOweek) %>% 
  mutate(weekly_courses = sum(dailycourses)) %>% 
  ungroup() %>% 
  select(ISOweek, weekly_courses)

#remove duplicates 
oseltamivir_weekly <- distinct(oseltamivir_weekly)

# repeat excluding returns
oseltamivir_noreturn <- oseltamivir_noreturn %>% 
  mutate(YEAR_MONTH = as.Date(paste(YEAR_MONTH, "01", sep=""), format = "%Y%m%d"))

oseltamivir_daily_noreturn <- oseltamivir_noreturn %>%
  group_by(as.Date(YEAR_MONTH)) %>%
  expand(Date = seq(floor_date(YEAR_MONTH, unit = "month"),
                    ceiling_date(YEAR_MONTH, unit="month")-days(1), by="day"), total_courses) %>%
  as.data.frame() %>% 
  mutate(ISOweek = ISOweek(Date),
         daysinmonth = days_in_month(Date))

# divide total courses by the number of days in a month
oseltamivir_daily_noreturn <- oseltamivir_daily_noreturn %>% 
  mutate(dailycourses = total_courses/daysinmonth)

# sum to weekly courses
oseltamivir_weekly_noreturn <- oseltamivir_daily_noreturn %>% 
  group_by(ISOweek) %>% 
  mutate(weekly_courses = sum(dailycourses)) %>% 
  ungroup() %>% 
  select(ISOweek, weekly_courses)

#remove duplicates 
oseltamivir_weekly_noreturn <- distinct(oseltamivir_weekly_noreturn)

# merge tables
weekly_inc_returns <- oseltamivir_weekly %>% 
  left_join(sari_weekly, by = c("ISOweek"="weekyear")) %>% 
  mutate(ISOweek = yearweek(ISOweek))

weekly_exc_returns <- oseltamivir_weekly_noreturn %>% 
  left_join(sari_weekly, by = c("ISOweek"="weekyear")) %>% 
  mutate(ISOweek = yearweek(ISOweek))

# 8. PCA df prep --------------------------------------
#convert quant and items to numeric
cols = c(9,21)
async_df_pca[,cols] = apply(async_df_pca[,cols], 2, function(x) as.numeric(as.character(x)))

async_df_pca <- async_df_pca %>% 
  mutate(DATASET = "async_df_pca",
         mg_dose = case_when(BNF_PRESENTATION_NAME == "Ebilfumin 75mg capsules" ~ 75,
                             BNF_PRESENTATION_NAME == "Oseltamivir 6mg/ml oral suspension sugar free" ~ 6,
                             BNF_PRESENTATION_NAME == "Oseltamivir 15mg/ml oral liquid" ~ 15,
                             BNF_PRESENTATION_NAME == "Oseltamivir 30mg capsules" ~ 30,
                             BNF_PRESENTATION_NAME == "Oseltamivir 45mg capsules" ~ 45,
                             BNF_PRESENTATION_NAME == "Oseltamivir 75mg capsules" ~ 75,
                             BNF_PRESENTATION_NAME == "Tamiflu 6mg/ml oral suspension" ~ 6,
                             BNF_PRESENTATION_NAME == "Tamiflu 30mg capsules" ~ 30,
                             BNF_PRESENTATION_NAME == "Tamiflu 45mg capsules" ~ 45,
                             BNF_PRESENTATION_NAME == "Tamiflu 75mg capsules" ~ 75,
                             TRUE ~ as.numeric(NA_character_)),
         quantity_mg = (mg_dose * TOTAL_QUANTITY),
         month = lubridate::ym(YEAR_MONTH),
         method = case_when(BNF_PRESENTATION_NAME == "Ebilfumin 75mg capsules" ~ "Tablet",
                            BNF_PRESENTATION_NAME == "Oseltamivir 6mg/ml oral suspension sugar free" ~ "Liquid",
                            BNF_PRESENTATION_NAME == "Oseltamivir 15mg/ml oral liquid" ~ "Liquid",
                            BNF_PRESENTATION_NAME == "Oseltamivir 30mg capsules" ~ "Tablet",
                            BNF_PRESENTATION_NAME == "Oseltamivir 45mg capsules" ~ "Tablet",
                            BNF_PRESENTATION_NAME == "Oseltamivir 75mg capsules" ~ "Tablet",
                            BNF_PRESENTATION_NAME == "Tamiflu 6mg/ml oral suspension" ~ "Liquid",
                            BNF_PRESENTATION_NAME == "Tamiflu 30mg capsules" ~ "Tablet",
                            BNF_PRESENTATION_NAME == "Tamiflu 45mg capsules" ~ "Tablet",
                            BNF_PRESENTATION_NAME == "Tamiflu 75mg capsules" ~ "Tablet",
                            TRUE ~ NA_character_),
         method = as.factor(method))

async_df_pca <- async_df_pca %>% 
  mutate(mls = case_when(UNIT_OF_MEASURE == "ml" ~ TOTAL_QUANTITY,
                         UNIT_OF_MEASURE == "capsule" ~ 0),
         mgs = case_when(UNIT_OF_MEASURE == "ml" ~ 0,
                         UNIT_OF_MEASURE == "capsule" ~ TOTAL_QUANTITY),
         total = case_when(UNIT_OF_MEASURE == "ml" ~ TOTAL_QUANTITY,
                           UNIT_OF_MEASURE == "capsule" ~ TOTAL_QUANTITY)) 

oseltamivir_summary_pca <- async_df_pca %>% 
  group_by(YEAR_MONTH) %>% 
  summarise(total_ml = sum(mls),
            total_capsule = sum(mgs),
            courses_ml = total_ml/65,
            courses_capsule = total_capsule/10,
            total_courses = courses_ml + courses_capsule) %>% 
  ungroup()

# convert prescriptions to estimated weekly counts
oseltamivir_summary_pca <- oseltamivir_summary_pca %>% 
  mutate(YEAR_MONTH = as.Date(paste(YEAR_MONTH, "01", sep=""), format = "%Y%m%d"))

oseltamivir_daily_pca <- oseltamivir_summary_pca %>%
  group_by(as.Date(YEAR_MONTH)) %>%
  expand(Date = seq(floor_date(YEAR_MONTH, unit = "month"),
                    ceiling_date(YEAR_MONTH, unit="month")-days(1), by="day"), total_courses) %>%
  as.data.frame() %>% 
  mutate(ISOweek = ISOweek(Date),
         daysinmonth = days_in_month(Date))

# divide total courses by the number of days in a month
oseltamivir_daily_pca <- oseltamivir_daily_pca %>% 
  mutate(dailycourses = total_courses/daysinmonth)

# sum to weekly courses
oseltamivir_weekly_pca <- oseltamivir_daily_pca %>% 
  group_by(ISOweek) %>% 
  mutate(weekly_courses = sum(dailycourses)) %>% 
  ungroup() %>% 
  select(ISOweek, weekly_courses)

#remove duplicates 
oseltamivir_weekly_pca <- distinct(oseltamivir_weekly_pca)

# 9. Proportions of admissions to prescriptions ----------------------------------
# Add column in sari_weekly to be the month
# Convert to class date

#oseltamivir_noreturn <- oseltamivir_noreturn %>%
#  mutate(YEAR_MONTH = lubridate::ym(YEAR_MONTH))

# merge sari_weekly with scmd and pca to obtain one df for each
sari_pca <- merge(x=sari_weekly, y=oseltamivir_weekly_pca, by.x="weekyear", by.y="ISOweek")
sari_scmd <- merge(x=sari_weekly, y=oseltamivir_weekly, by.x="weekyear", by.y="ISOweek")
sari_scmd_nr <- merge(x=sari_weekly, y=oseltamivir_weekly_noreturn, by.x="weekyear", by.y="ISOweek")

#add new column for proportion of admissions to prescriptions
sari_pca <- sari_pca %>%
  mutate(adm_per_c = admissions/weekly_courses) %>%
  mutate(c_per_adm = weekly_courses/admissions)

sari_scmd <- sari_scmd %>%
  mutate(adm_per_c = admissions/weekly_courses) %>%
  mutate(c_per_adm = weekly_courses/admissions)

sari_scmd_nr <- sari_scmd_nr %>%
  mutate(adm_per_c = admissions/weekly_courses) %>%
  mutate(c_per_adm = weekly_courses/admissions)

# 10. Time series ---------------------------------------------------------------
# convert to a tsibble
#filtering to be winter 2019 to winter 2023
#SCMD
oseltamivir_summary <- oseltamivir_summary %>% 
  mutate(YEAR_MONTH = as.Date(paste(YEAR_MONTH, "01", sep=""), format = "%Y%m%d"))

oseltamivir_summary_scmd_filt <- oseltamivir_summary %>% 
  filter(YEAR_MONTH >= "2019-10-01" & YEAR_MONTH <= "2023-04-01") %>%
  as_tsibble(index = YEAR_MONTH)

oseltamivir_noreturn <- oseltamivir_noreturn %>% 
  mutate(YEAR_MONTH = as.Date(paste(YEAR_MONTH, "01", sep=""), format = "%Y%m%d"))

oseltamivir_noreturn <- oseltamivir_noreturn %>% 
  as_tsibble(index = YEAR_MONTH)

sari_scmd_filt <- sari_scmd %>% 
  filter(weekdate >= "2019-10-01" & weekdate <= "2023-04-01") %>%
  as_tsibble(index = weekdate)

sari_scmd_nr_filt <- sari_scmd_nr %>%
  filter(weekdate >= "2019-10-01" & weekdate <= "2023-04-01") %>%
  as_tsibble(index = weekdate)


#PCA
oseltamivir_summary_pca <- oseltamivir_summary_pca %>% 
  mutate(YEAR_MONTH = as.Date(paste(YEAR_MONTH, "01", sep=""), format = "%Y%m%d"))

oseltamivir_summary_pca_filt <- oseltamivir_summary_pca %>% 
  filter(YEAR_MONTH >= "2019-10-01" & YEAR_MONTH <= "2023-04-01") %>%
  as_tsibble(index = YEAR_MONTH)

sari_pca_filt <- sari_pca %>% 
  filter(weekdate >= "2019-10-01" & weekdate <= "2023-04-01") %>%
  as_tsibble(index = weekdate)


#SARI
sari_weekly <- sari_weekly %>% 
  as_tsibble(index = weekdate)

sari_weekly_filt <- sari_weekly %>%
  filter(weekdate >= "2019-10-07" & weekdate <= "2023-04-03")

# 11. Time series plots ---------------------------------------------------------------------
# Generate plots
plot_colours <- c("#28A197","#F46A25", "#801650", "#12436D","#A285D1", "#3D3D3D")
# estimated admissions, SCMD courses of oseltamivir, one axis

ggplot(NULL) +
  geom_line(data = sari_pca_filt, aes(y=weekly_courses, colour = "Estimated weekly courses of oseltamivir in community care",
                                      x = weekdate), linewidth = 0.6) +
  geom_line(data = sari_scmd_filt, aes(y=weekly_courses, colour = "Estimated weekly courses of oseltamivir in secondary care",
                                       x = weekdate), linewidth = 0.6) +
  geom_line(data = sari_weekly_filt, aes(y=admissions, colour = "Estimated admissions", 
                                         x = weekdate), linewidth = 0.6) +
  scale_color_manual(name="", values = plot_colours) +
  scale_y_continuous("") +
  scale_x_date("Month", breaks = "3 month", date_labels = "%b %y", minor_breaks ="1 month") +
  ggtitle("Estimated influenza admissions in comparison to estimated weekly oseltamivir prescriptions in community and secondary care", 
          subtitle = "Including returns of supply in secondary care") +
  theme_bw()
ggsave("oseltamivir_sari_pca_scmd_inc.png", width = 12, height = 5)

# 2 axis plot for admissions (unable to index due to different timeframes)
ylim.prim <- c(min(sari_weekly_filt$admissions, na.rm = TRUE), 
               max(sari_weekly_filt$admissions, na.rm = TRUE)) 
ylim.sec <- c(min(sari_scmd_filt$weekly_courses, na.rm = TRUE), 
              max(sari_scmd_filt$weekly_courses, na.rm = TRUE))
b <- diff(ylim.prim)/diff(ylim.sec)
a <- ylim.prim[1] - b*ylim.sec[1]

ggplot(NULL) +
  geom_line(data = sari_weekly_filt, aes(y=admissions, colour = "Estimated admissions", 
                                         x = weekdate), linewidth = 0.6) +
  geom_line(data = sari_scmd_filt, aes(y=(a + (weekly_courses*b)), 
                                       colour = "Estimated weekly courses of oseltamivir in secondary care",
                                       x = weekdate), linewidth = 0.6) +
  geom_line(data = sari_pca_filt, aes(y=(a + (weekly_courses*b)), 
                                      colour = "Estimated weekly courses of oseltamivir in community care",
                                      x = weekdate), linewidth = 0.6) +
  scale_color_manual(name="", values = plot_colours) +
  scale_y_continuous("Estimated admissions", sec.axis = sec_axis(~ (. - a)/b, name = "Estimated weekly courses")) +
  scale_x_date("Month", breaks = "3 month", date_labels = "%b %y", minor_breaks ="1 month") +
  ggtitle("Estimated influenza admissions in comparison to estimated weekly oseltamivir prescriptions in community and secondary care", 
          subtitle = "Including returns of supply in secondary care") +
  theme_bw()
ggsave("oseltamivir_inc_returns_sari2axes.png", width = 12, height = 5)

