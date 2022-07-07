#### Loading Libraries & Environment Setup####

library(flexdashboard) #interactive dashboards for R
library(dplyr) #Data manipulation
library(readxl) #read in Excel files
library(RSQLite) #SQLite data connection
library(haven) # enables R to read and write various data formats - SAS, SPSS, Stata
library(ggplot2) # Creating graphics
library(devtools) # access to github
#devtools::install_github("arthur-shaw/susoapi")
library(susoapi) # R wrapper function for each Survey Solutions API endpoint
library(readr) #  provide a fast and friendly way to read rectangular data

repository <- file.path(dirname(rstudioapi::getSourceEditorContext()$path))
setwd(repository)

mydb <- dbConnect(RSQLite::SQLite(), "data/secure/sqlite/bec2022.sqlite")
con <- dbConnect(RSQLite::SQLite(), "data/secure/sqlite/bec2022_monitor.sqlite")

#### API Configuration ####
# Assign Credentials to the variables (System Variables)
server_name <- Sys.getenv("SUSO_SERVER")
server_user <- Sys.getenv("BEC_USER")
server_password <- Sys.getenv("BEC_PASSWORD")

# Assign credentials to the SUSOAPI function "set_credentials
set_credentials(
  server = server_name,
  user = server_user,
  password = server_password
  #workspace = "bec"
)

#Retrieve all questions being assigned to the BEC_USER
all_questionnaires <- get_questionnaires(workspace = "bec")

#create JOB Id for purpose of exporting all data from the server
start_export(
  qnr_id = all_questionnaires$questionnaireId,
  export_type = "Tabular",
  interview_status = "All",
  include_meta = TRUE,
  workspace = "bec"
) -> started_job_id

# Get export job details ID in preparation in export data
get_export_job_details(job_id = started_job_id, workspace = "bec")

# Export data using the export ID to a zip file
get_export_file(
  job_id = started_job_id,
  path = "data/secure/",
  workspace = "bec",
)

#### Process Incoming Data ####

## Read data into R from downloaded zip file ##
main <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "business_census2022.tab"))
capital_goods <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "capital_goods.tab"))
exp_details <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "exp_details.tab"))
exp_range <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "exp_range.tab"))
finaAcess_roser <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "finaAcess_roser.tab"))
info_activities <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "info_activities.tab"))
info_other_business <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "info_other_business.tab"))
mobMoney_info <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "mobMoney_info.tab"))
other_cost <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "other_cost.tab"))
other_invest <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "other_invest.tab"))
ref_yearDetails <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "ref_yearDetails.tab"))
register_id <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "register_id.tab"))
errors <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "interview__errors.tab"))
interview_actions <- read_delim(unzip("data/secure/business_census2022_2_Tabular_All.zip", "interview__actions.tab"))

## Cleaning data frames read from server
# 1. Renaming Field = interview__key in df to  id
# 2. Rewrite df to mysqlite db = mydb
colnames(main)[1] <- "id"
colnames(capital_goods)[1] <- "id"
colnames(exp_details)[1] <- "id"
colnames(exp_range)[1] <- "id"
colnames(finaAcess_roser)[1] <- "id"
colnames(info_activities)[1] <- "id"
colnames(info_other_business)[1] <- "id"
colnames(mobMoney_info)[1] <- "id"
colnames(other_cost)[1] <- "id"
colnames(other_invest)[1] <- "id"
colnames(ref_yearDetails)[1] <- "id"
colnames(register_id)[1] <- "id"
colnames(errors)[1] <- "id"
colnames(interview_actions)[1] <- "id"

# 2. Rewrite df to mydb with new column name=id
dbWriteTable(mydb, "main", main, overwrite=TRUE)
dbWriteTable(mydb, "capital_goods", capital_goods, overwrite=TRUE)
dbWriteTable(mydb, "exp_details", exp_details, overwrite=TRUE)
dbWriteTable(mydb, "exp_range", exp_range, overwrite=TRUE)
dbWriteTable(mydb, "finaAcess_roser", finaAcess_roser, overwrite=TRUE)
dbWriteTable(mydb, "info_activities", info_activities, overwrite=TRUE)
dbWriteTable(mydb, "info_other_business", info_other_business, overwrite=TRUE)
dbWriteTable(mydb, "mobMoney_info", mobMoney_info, overwrite=TRUE)
dbWriteTable(mydb, "other_cost", other_cost, overwrite=TRUE)
dbWriteTable(mydb, "other_invest", other_invest, overwrite=TRUE)
dbWriteTable(mydb, "ref_yearDetails", ref_yearDetails, overwrite=TRUE)
dbWriteTable(mydb, "register_id", register_id, overwrite=TRUE)
dbWriteTable(mydb, "errors", errors, overwrite=TRUE)
dbWriteTable(mydb, "interview_actions", interview_actions, overwrite=TRUE)

## Remove temporary files from main directory ##
file.remove(list.files(pattern = "*.tab"))

## Remove unwanted columns ##
# Columns 35 to Column 49 - busiName_other__0 - busiName_other__14
main = select(main, -35:-49)

## Load Look up Tables from bec_classification.xlsx (directory: data/open)
# Data is read from a single spreadsheet that has multiples sheets# 
ref_letter <- read_excel("data/open/bec_classification.xlsx", sheet = "Reference_Letter") # Reference letter of assigned teams
position <- read_excel("data/open/bec_classification.xlsx", sheet = "position") # Position of Interviewee
owner_sex <- read_excel("data/open/bec_classification.xlsx", sheet = "owner_sex") # Sex of Owner
owner_character <- read_excel("data/open/bec_classification.xlsx", sheet = "owner_character") # Characteristics of owner
employee_sex <- read_excel("data/open/bec_classification.xlsx", sheet = "employee_sex") # sex of employee
registered <- read_excel("data/open/bec_classification.xlsx", sheet = "registered") # Business Registered
province <- read_excel("data/open/bec_classification.xlsx", sheet = "province") # Office location: Province
island <- read_excel("data/open/bec_classification.xlsx", sheet = "island") # Office location: island
area_council <- read_excel("data/open/bec_classification.xlsx", sheet = "area_council") # Office location: area_council
institution_reg <- read_excel("data/open/bec_classification.xlsx", sheet = "institution_reg") # Institution of Registration
legal_status <- read_excel("data/open/bec_classification.xlsx", sheet = "legal_status") # Legal status of business
place_of_operation <- read_excel("data/open/bec_classification.xlsx", sheet = "place_of_operation") # Place of operation.
account_keeping <- read_excel("data/open/bec_classification.xlsx", sheet = "account_keeping") # How accounts are kept
reference_year <- read_excel("data/open/bec_classification.xlsx", sheet = "reference_year") # Reference Year
major_label_vansic <- read_excel("data/open/bec_classification.xlsx", sheet = "major_label_vansic") # Major Label VANSIC
sub_major_label_vansic <- read_excel("data/open/bec_classification.xlsx", sheet = "sub_major_label_vansic") # Sub-Major Label VANSIC
minor_label_vansic <- read_excel("data/open/bec_classification.xlsx", sheet = "minor_label_vansic") # Minor Sub-Major Label VANSIC
unit_label_vansic <- read_excel("data/open/bec_classification.xlsx", sheet = "unit_label_vansic") # Unit Label VANSIC
vnsic_label_infovansic <- read_excel("data/open/bec_classification.xlsx", sheet = "vnsic_label_infovansic") # VANSIC
imp_brdclose <- read_excel("data/open/bec_classification.xlsx", sheet = "imp_brdclose") # COVID-19 Reaction
imp_finstatus <- read_excel("data/open/bec_classification.xlsx", sheet = "imp_finstatus") # COVID-19 Impact Financial Status
imp_situation <- read_excel("data/open/bec_classification.xlsx", sheet = "imp_situation") # Part 2. C3
overcome_problem <- read_excel("data/open/bec_classification.xlsx", sheet = "overcome_problem") # Part 2. C4 - Overcome problem
operation_Noexp <- read_excel("data/open/bec_classification.xlsx", sheet = "operation_Noexp") # Q. IN2a
informal <- read_excel("data/open/bec_classification.xlsx", sheet = "informal") # Q. IN3a.
bus_activities <- read_excel("data/open/bec_classification.xlsx", sheet = "bus_activities") # Q. IN4.
major_Infolabel_vanisic <- read_excel("data/open/bec_classification.xlsx", sheet = "major_Infolabel_vanisic") # Q. IN4a.
sub_majorlabel_info_vanisic <- read_excel("data/open/bec_classification.xlsx", sheet = "sub_majorlabel_info_vanisic") #IN4b: Sub Major Label Vansic
minor_label_infovansic <- read_excel("data/open/bec_classification.xlsx", sheet = "minor_label_infovansic") #IN4c: Group under the Division. (VANSIC)
unit_label_infovansic <- read_excel("data/open/bec_classification.xlsx", sheet = "unit_label_infovansic") #IN4d: Class under the Group. (VANSIC)
vansic_label_infovansic <- read_excel("data/open/bec_classification.xlsx", sheet = "vansic_label_infovansic") #IN4e: VANSIC under the CLASS. (VANSIC)
infor_paidEmp <- read_excel("data/open/bec_classification.xlsx", sheet = "infor_paidEmp") #IN5.: Paid Employee (yes/no)
unpaid_emp <- read_excel("data/open/bec_classification.xlsx", sheet = "unpaid_emp") #IN6.: unpaid workers (yes/no)
hhld_business <- read_excel("data/open/bec_classification.xlsx", sheet = "hhld_business") #EI1.: Business Operate Within the Households
hhld_exp <- read_excel("data/open/bec_classification.xlsx", sheet = "hhld_exp") #EI2: expenditure on the cost of utilities
finance_source <- read_excel("data/open/bec_classification.xlsx", sheet = "finance_source") #F1.: Source of Finance
mobile_money_service <- read_excel("data/open/bec_classification.xlsx", sheet = "mobile_money_service") #F4.: Mobile Money Services
rank_of_services <- read_excel("data/open/bec_classification.xlsx", sheet = "rank_of_services") #F4b: Rank of Mobile Money Services


# Writing in new classification tables read from excel file - bec_classification.xlsx above to the database=mydb 
dbWriteTable(mydb, "ref_letter", ref_letter, overwrite=TRUE) # Reference letter of assigned teams
dbWriteTable(mydb, "position", position, overwrite=TRUE)# Position of Interviewee
dbWriteTable(mydb, "owner_sex", owner_sex, overwrite=TRUE) # Sex of Owner
dbWriteTable(mydb, "owner_character", owner_character, overwrite=TRUE) # Characteristics of owner
dbWriteTable(mydb, "employee_sex", employee_sex, overwrite=TRUE) # sex of employee
dbWriteTable(mydb, "registered", registered, overwrite=TRUE) # Business Registered
dbWriteTable(mydb, "province", province, overwrite=TRUE) # Office location: Province
dbWriteTable(mydb, "island", island, overwrite=TRUE) # Office location: island
dbWriteTable(mydb, "area_council", area_council, overwrite=TRUE) # Office location: area_council
dbWriteTable(mydb, "institution_reg", institution_reg, overwrite=TRUE) # Institution of Registration
dbWriteTable(mydb, "legal_status", legal_status, overwrite=TRUE) #  Legal status of business
dbWriteTable(mydb, "place_of_operation", place_of_operation, overwrite=TRUE) # Place of operation.
dbWriteTable(mydb, "account_keeping", account_keeping, overwrite=TRUE) # How accounts are kept
dbWriteTable(mydb, "reference_year", reference_year, overwrite=TRUE) # Reference Year
dbWriteTable(mydb, "major_label_vansic", major_label_vansic, overwrite=TRUE) # Major Label VANSIC
dbWriteTable(mydb, "sub_major_label_vansic", sub_major_label_vansic, overwrite=TRUE) # Sub-Major Label VANSIC
dbWriteTable(mydb, "minor_label_vansic", minor_label_vansic, overwrite=TRUE) # Minor Sub-Major Label VANSIC
dbWriteTable(mydb, "unit_label_vansic", unit_label_vansic, overwrite=TRUE) #  Unit Label VANSIC
dbWriteTable(mydb, "vnsic_label_infovansic", vnsic_label_infovansic, overwrite=TRUE) # VANSIC
dbWriteTable(mydb, "imp_brdclose", imp_brdclose, overwrite=TRUE) #  COVID-19 Reaction
dbWriteTable(mydb, "imp_finstatus", imp_finstatus, overwrite=TRUE) #   COVID-19 Impact Financial Status
dbWriteTable(mydb, "imp_situation", imp_situation, overwrite=TRUE) # Part 2. C3
dbWriteTable(mydb, "overcome_problem", overcome_problem, overwrite=TRUE) # Part 2. C4 - Overcome problem
dbWriteTable(mydb, "operation_Noexp", operation_Noexp, overwrite=TRUE) # Q. IN2a
dbWriteTable(mydb, "informal", informal, overwrite=TRUE) # Q. IN3a.
dbWriteTable(mydb, "bus_activities", bus_activities, overwrite=TRUE)  # Q. IN4.
dbWriteTable(mydb, "major_Infolabel_vanisic", major_Infolabel_vanisic, overwrite=TRUE)   # Q. IN4a: Major Information Label Vansic
dbWriteTable(mydb, "sub_majorlabel_info_vanisic", sub_majorlabel_info_vanisic, overwrite=TRUE) #IN4b: Sub Major Label Vansic
dbWriteTable(mydb, "minor_label_infovansic", minor_label_infovansic, overwrite=TRUE) #IN4c.: Group under the Division. (VANSIC)
dbWriteTable(mydb, "unit_label_infovansic", unit_label_infovansic, overwrite=TRUE) #IN4d: Class under the Group. (VANSIC)
dbWriteTable(mydb, "vansic_label_infovansic", vansic_label_infovansic, overwrite=TRUE) #IN4e: VANSIC under the CLASS. (VANSIC)
dbWriteTable(mydb, "infor_paidEmp", infor_paidEmp, overwrite=TRUE) #IN5.: Paid Employee (yes/no)
dbWriteTable(mydb, "unpaid_emp", unpaid_emp, overwrite=TRUE) #IN5.: Paid Employee (yes/no)
dbWriteTable(mydb, "hhld_business", hhld_business, overwrite=TRUE) #EI1.: Business Operate Within the Households
dbWriteTable(mydb, "hhld_exp", hhld_exp, overwrite=TRUE) #EI2: expenditure on the cost of utilities
dbWriteTable(mydb, "finance_source", finance_source, overwrite=TRUE) #F1.: Source of Finance
dbWriteTable(mydb, "mobile_money_service", mobile_money_service, overwrite=TRUE) #F4.: Mobile Money Services
dbWriteTable(mydb, "rank_of_services", rank_of_services, overwrite=TRUE) #F4b: Rank of Mobile Money Services
















province <- read.csv("data/open/province.csv")
prov <- dbGetQuery(mydb, "SELECT province, COUNT(id) AS total FROM main WHERE province > 0 GROUP BY province")

prov_results <- merge(prov, province, by = "province")

p<-ggplot(prov_results, aes(x=province_desc, y=total, fill=total)) +
  geom_bar(stat="identity")+theme_minimal()
p

errors_detect <- dbGetQuery(mydb, "SELECT variable, COUNT(id) AS total FROM errors GROUP BY variable ")

p<-ggplot(errors_detect, aes(x=variable, y=total, fill=total)) +
  geom_bar(stat="identity")+theme_minimal()
p

myvars <- c("interview__key", "originator", "action")
interviewActions <- interview_actions[myvars]


dbDisconnect(mydb)
