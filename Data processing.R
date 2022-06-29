library(flexdashboard)
library(dplyr) #Data manipulation
library(readxl) #read in Excel files
library(RSQLite) #SQLite data connection
library(haven)
library(susoapi)
library(ggplot2)

repository <- file.path(dirname(rstudioapi::getSourceEditorContext()$path))
setwd(repository)

mydb <- dbConnect(RSQLite::SQLite(), "data/secure/sqlite/bec2022.sqlite")

server_name <- Sys.getenv("SUSO_SERVER")
server_user <- Sys.getenv("BEC_USER")
server_password <- Sys.getenv("BEC_PASSWORD")

set_credentials(
  server = server_name,
  user = server_user,
  password = server_password
)

all_questionnaires <- get_questionnaires(workspace = "bec")

start_export(
  qnr_id = "bf962319d179467babb153516f46018b$2",
  export_type = "Tabular",
  interview_status = "All",
  include_meta = TRUE,
  workspace = "bec"
) -> started_job_id

get_export_job_details(job_id = started_job_id, workspace = "bec")

get_export_file(
  job_id = started_job_id,
  path = "data/secure/",
  workspace = "bec",
)

#Read data into R from downloaded zip file
main <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "business_census2022.tab"))
capital_goods <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "capital_goods.tab"))
exp_details <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "exp_details.tab"))
exp_range <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "exp_range.tab"))
finaAcess_roser <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "finaAcess_roser.tab"))
info_activities <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "info_activities.tab"))
info_other_business <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "info_other_business.tab"))
mobMoney_info <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "mobMoney_info.tab"))
other_cost <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "other_cost.tab"))
other_invest <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "other_invest.tab"))
ref_yearDetails <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "ref_yearDetails.tab"))
register_id <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "register_id.tab"))
errors <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "interview__errors.tab"))
interview_actions <- readr::read_delim(unz("data/secure/business_census2022_1_Tabular_All.zip", "interview__actions.tab"))
#Write imported data to SQLite database

dbWriteTable(mydb, "main", main, overwrite = TRUE)
dbWriteTable(mydb, "exp_details", exp_details, overwrite=TRUE)
dbWriteTable(mydb, "capital_goods", capital_goods, overwrite=TRUE)
dbWriteTable(mydb, "exp_range", exp_range, overwrite=TRUE)
dbWriteTable(mydb, "finaAcess_roser", finaAcess_roser, overwrite=TRUE)
dbWriteTable(mydb, "info_activities", info_activities, overwrite=TRUE)
dbWriteTable(mydb, "info_other_business", info_other_business, overwrite=TRUE)
dbWriteTable(mydb, "mobMoney_info", mobMoney_info, overwrite=TRUE)
dbWriteTable(mydb, "other_cost", other_cost, overwrite=TRUE)
dbWriteTable(mydb, "other_invest", other_invest, overwrite=TRUE)
dbWriteTable(mydb, "ref_yearDetails", ref_yearDetails, overwrite=TRUE)
dbWriteTable(mydb, "register_id", register_id, overwrite=TRUE)
dbWriteTable(mydb, "errors", errors, overwrite = TRUE)
dbWriteTable(mydb, "interview_actions", interview_actions, overwrite = TRUE)

province <- read.csv("data/open/province.csv")
prov <- dbGetQuery(mydb, "SELECT province, COUNT(interview__key) AS total FROM main WHERE province > 0 GROUP BY province")

prov_results <- merge(prov, province, by = "province")

p<-ggplot(prov_results, aes(x=province_desc, y=total, fill=total)) +
  geom_bar(stat="identity")+theme_minimal()
p

errors_detect <- dbGetQuery(mydb, "SELECT variable, COUNT(interview__key) AS total FROM errors GROUP BY variable ")

p<-ggplot(errors_detect, aes(x=variable, y=total, fill=total)) +
  geom_bar(stat="identity")+theme_minimal()
p

interviewer_errors <- interview_actions(c("interview__key", "originator"))




dbDisconnect(mydb)