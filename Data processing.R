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
#install.packages("zoo")
library(zoo) # as.date() converting numeric date to normal day values

options(scipen = 100, digits = 4) #prevent scientific notation

repository <- file.path(dirname(rstudioapi::getSourceEditorContext()$path))
setwd(repository)

mydb <- dbConnect(RSQLite::SQLite(), "data/secure/sqlite/bec2022.sqlite")

#### API Configuration ####
# Assign Credentials to the variables (System Variables)
server_name <- Sys.getenv("SUSO_SERVER")
server_user <- Sys.getenv("BEC_USER")
server_password <- Sys.getenv("BEC_PASSWORD")

# Assign credentials to the SUSOAPI function "set_credentials
set_credentials(
  server = server_name,
  user = server_user,
  password = server_password,
  workspace = "bec"
)

#Retrieve all questions being assigned to the BEC_USER
all_questionnaires <- get_questionnaires(workspace = "bec")

#create JOB Id for purpose of exporting data from the server
start_export(
  qnr_id = "bf962319d179467babb153516f46018b$2",
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

#### Cleaning: main Dataframe ####

## 1. Remove unwanted columns ##
main = select(main, -busiName_other__0:-busiName_other__14, -bus_activities__1:-bus_activities__21, -hhld_exp__1:-hhld_exp__7, -finance_source__1:-finance_source__7, -mobile_money_service__1:-mobile_money_service__4)
ref_yearDetails = select(ref_yearDetails, -utilities_exp__1:-utilities_exp__7) # Roster repetition

## 2. Change numeric datatypes to factor in all Categorical Fields
main <- mutate_at(main, vars(ref_letter, ref_number, second_number, email, owner_sex, owner_character,
                             second_email, postal, employee_sex, position, registered, ynRegistered, head_office, 
                             otherUnits_acc, province, island, area_council, permit_number, legal_status, place_of_operation, 
                             account_keeping, major_label_vansic,
                             sub_major_label_vansic, minor_label_vansic, unit_label_vansic,
                             vnsic_label_infovansic, imp_brdclose, imp_finstatus, imp_situation__1, 
                             imp_situation__2, imp_situation__3, imp_situation__4, imp_situation__5,
                             overcome_problem__1, overcome_problem__2, overcome_problem__3, overcome_problem__4, 
                             overcome_problem__5, overcome_problem__6, overcome_problem__7, overcome_problem__8, 
                             business_operate, operation_Noexp__1, operation_Noexp__2, operation_Noexp__3,
                             operation_Noexp__4, operation_Noexp__5, same_business, informal__1,
                             informal__2, informal__3, informal__4, informal__5, 
                             major_Infolabel_vanisic, sub_majorlabel_info_vanisic, minor_label_infovansic, 
                             unit_label_infovansic, vansic_label_infovansic, infor_paidEmp, unpaid_emp,
                             hhld_business, excess_exp, business_develop, business_suceed, assisst_bank, 
                             gov_assist, bank_services, bank_regUses, aware_bankService, info_bank, 
                             insurance, insurance_poilcy, insurance_noPolicy, 
                             g4_business_gps__Accuracy, g4_business_gps__Altitude, g4_business_gps__Latitude, g4_business_gps__Longitude), as.factor)

## 3. Changing selected columns/fields above to integer for calculation purposes. E.g. Age, Day of Birth, Number of Business Establishments
main <- mutate_at(main, vars(age, day_of_birth, business_locate, male_inforEmp, female_inforEmp, 
                             paid_empTot, hrs_unpaidMale, unpaid_Maleemp, unpaid_Fememp, unpaid_empSize, 
                             hrs_unpaidMale, hrs_unpaidFemale, average_hrs), as.integer)

# Percentage of Expense of Utilities - exp_range data frame
#exp_range <- mutate_at(exp_range, vars (exp_utilities), as.factor)
exp_range <- mutate_at(exp_range, vars(exp_utilities), as.integer)

# Converting utilities_cost column in exp_details data frame into integer
#exp_details <- mutate_at(exp_details, vars (utilities_cost), as.factor)
exp_details <- mutate_at(exp_details, vars(utilities_cost), as.integer)


# Expense & Income

ref_yearDetails <- mutate_at(ref_yearDetails, vars(wage_bill, income_resale, cost_resale, 
                                                   income, expense, tot_paid_emp, sales_goods_services, local_emp, 
                                                   foreign_emp, employee_comp, employee_comp, mang_vanmale, skill_vanmale, 
                                                   unskil_vanmale, mang_vanfemale, skill_vanfemale, unskil_vanfemale, mang_formale, 
                                                   skill_formale, unskil_formale, mang_forfemale, skill_forfemale, unskil_forfemale, 
                                                   transport_cost, equip_cost, contract_opt, contract_cost, capital_exp, 
                                                   cost_construct, capital_inc, inc_construct, rent_recieved, 
                                                   rent_paid, interest_recieved, interest_paid, subsidy, tax_paid), as.integer)

other_cost <- mutate_at(other_cost, vars(cost_otherExp), as.integer)
other_invest <- mutate_at(other_invest, vars(cost_otherInvest), as.integer)
capital_goods <- mutate_at(capital_goods, vars(cost_otherCapital), as.integer)


## 4. Remove NAs from dataset and replace with blanks

# Category set to Blank
# Numeric -  set to 0s
#main <- sapply(main, as.character)
#main[is.na(main)] <- " "

#library(tidyr)
#main %>% 
 # mutate_at(vars(major_label_vansic), replace_na, '')



## Rewrite new amended df into database
dbWriteTable(mydb, "main", main, overwrite=TRUE)
dbWriteTable(mydb, "ref_yearDetails", ref_yearDetails, overwrite=TRUE)
dbWriteTable(mydb, "exp_range", exp_range, overwrite=TRUE)
dbWriteTable(mydb, "exp_details", exp_details, overwrite=TRUE)
dbWriteTable(mydb, "other_cost", other_cost, overwrite=TRUE)
dbWriteTable(mydb, "other_invest", other_invest, overwrite=TRUE)
dbWriteTable(mydb, "capital_goods", capital_goods, overwrite=TRUE)


## Load Look up Tables from bec_classification.xlsx (directory: data/open)
# Data is read from a single spreadsheet that has multiples sheets# 
ref_letter <- read_excel("data/open/bec_classification.xlsx", sheet = "ref_letter") # Reference letter of assigned teams
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
interview_status <- read_excel("data/open/bec_classification.xlsx", sheet = "interview_status") #Lookup interview status for fieldwork monitoring
enumerators <- read_excel("data/open/bec_classification.xlsx", sheet = "enumerators") #enumerators



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
dbWriteTable(mydb, "interview_status", interview_status, overwrite=TRUE) #Lookup interview status for fieldwork monitoring
dbWriteTable(mydb, "enumerators", enumerators, overwrite=TRUE) #Enumerators

#### Fieldwork Monitoring ####
fwm <- dbGetQuery(mydb, "Select main.id,
                  interview_status.interview_status_desc,
                  interview_actions.originator,
                  interview_actions.responsible__name,
                  enumerators.enumdesc,
                  interview_actions.date,
                  main.start_time,
                  main.start_end_time,
                  ref_letter.ref_number_desc AS Ref_Letter,
                  main.ref_number,
                  main.name_businesEsta AS Business_Name
                  
                  FROM
                  
                  interview_actions
                  
                  INNER JOIN main ON interview_actions.id = main.id
                  INNER JOIN interview_status ON main.interview__status = interview_status.interview_status
                  INNER JOIN enumerators ON interview_actions.originator = enumerators.enum
                  INNER JOIN ref_letter ON main.ref_letter = ref_letter.ref_number
                  
                  GROUP BY main.id
                  
                  ") 


#library(lubridate) # to use the mdy_hms function to convert character date to timestamp

# Formatting date fields to calculate the difference of times and interview was made

#fwm$start_time <- gsub("T"," ",as.character(fwm$start_time))
#fwm$start_end_time <- gsub("T"," ",as.character(fwm$start_end_time))

# Removing 
#fwm$start_time <- sub(".*?\\T", "", fwm$start_time)
#fwm <- fwm %>% mutate(start_time = mdy_hms(start_time))
#difftime(fwm$start_time, fwm$start_end_time, units = "mins")



fwm$date <- as.Date(fwm$date)
write.csv(fwm, "data/open/fieldwork.csv", row.names = FALSE)
write.csv(interview_actions, "data/open/interview_actions.csv", row.names = FALSE)

#write.csv(main, "data/open/main.csv", row.names = FALSE)
#write.csv(capital_goods, "data/open/capital_goods.csv", row.names = FALSE)
#write.csv(exp_details, "data/open/exp_details.csv", row.names = FALSE)
#write.csv(exp_range, "data/open/exp_range.csv", row.names = FALSE)
#write.csv(finaAcess_roser, "data/open/finaAcess_roser.csv", row.names = FALSE)
#write.csv(info_activities, "data/open/info_activities.csv", row.names = FALSE)
#write.csv(info_other_business, "data/open/info_other_business.csv", row.names = FALSE)
#write.csv(mobMoney_info, "data/open/mobMoney_info.csv", row.names = FALSE)
#write.csv(other_cost, "data/open/other_cost.csv", row.names = FALSE)
#write.csv(other_invest, "data/open/other_invest.csv", row.names = FALSE)
#write.csv(ref_yearDetails, "data/open/ref_yearDetails.csv", row.names = FALSE)
#write.csv(register_id, "data/open/register_id.csv", row.names = FALSE)
#write.csv(errors, "data/open/errors.csv", row.names = FALSE)
#write.csv(interview_actions, "data/open/interview_actions.csv", row.names = FALSE)


#### BEC TABULATION ####

# 1. Number of Establishments in each Industry by Province in 2019 and 2021.

Table_1 <- dbGetQuery(mydb, "SELECT 
                                                  area_council.area_council_desc AS Area_Council,
                                                  reference_year.reference_year_desc AS Reference_Year,
                                                  province.province_desc AS Province,
                                                  major_label_vansic.major_label_vansic_desc,
                                                  COUNT(*) as Number_Of_Establishments
                                                  
                                                  
                                                  FROM
                                                  
                                                  main
                                                  
                                                  INNER JOIN area_council ON main.area_council = area_council.area_council
                                                  INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                                                  INNER JOIN province ON main.province = province.province
                                                  INNER JOIN ref_yearDetails on main.id = ref_yearDetails.id
                                                  INNER JOIN reference_year on ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                                                  
                                                  GROUP BY area_council.area_council
                                                  
                                                  
                                    ")

# Table 2: Number of Establishments in Vanuatu by Size and Industry (Significant, Large, Medium, Small, Micro & Self-Employed) in 2019 and 2021
Table_2 <- dbGetQuery(mydb, "SELECT 
                                    major_label_vansic.major_label_vansic_desc,
                                    ref_yearDetails.tot_paid_emp AS Num_Of_Paid_Employees,
                                    reference_year.reference_year_desc AS Reference_Year
                      
                      FROM 
                      
                      main
                      
                      INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                      INNER JOIN ref_yearDetails ON ref_yearDetails.id = main.id
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      
                      GROUP BY major_label_vansic.major_label_vansic_desc
                      
                      HAVING reference_year.reference_year_desc = '2019' OR reference_year.reference_year_desc = '2021'
                      
                      ORDER BY ref_yearDetails.tot_paid_emp DESC
                      ")

# Table 3: Total number of Establishments by Legal Status and Industry in 2019
Table_3 <- dbGetQuery(mydb, "SELECT 
                                    major_label_vansic.major_label_vansic_desc AS Major_Industry,
                                    reference_year.reference_year_desc AS Reference_Year,
                                    legal_status.legal_status_desc AS Legal_Status,
                                    COUNT(*) AS Total_Number
                                    
                      
                      FROM 
                      
                      main
                      
                      INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                      INNER JOIN ref_yearDetails ON ref_yearDetails.id = main.id
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      INNER JOIN legal_status ON main.legal_status = legal_status.legal_status
                      
                      GROUP BY legal_status.legal_status
                      
                      HAVING reference_year.reference_year_desc = '2019'
                      
                      ")


# Table 4: Total number of Establishments by Legal Status and Industry in 2021
Table_4 <- dbGetQuery(mydb, "SELECT 
                                    major_label_vansic.major_label_vansic_desc AS Major_Industry,
                                    reference_year.reference_year_desc AS Reference_Year,
                                    legal_status.legal_status_desc AS Legal_Status,
                                    COUNT(*) AS Total_Number
                                    
                      
                      FROM 
                      
                      main
                      
                      INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                      INNER JOIN ref_yearDetails ON ref_yearDetails.id = main.id
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      INNER JOIN legal_status ON main.legal_status = legal_status.legal_status
                      
                      GROUP BY legal_status.legal_status
                      
                      HAVING reference_year.reference_year_desc = '2021'
                      
                      ")

# Table 5: Total Sales by Size and Industry in 2019 and 2021

Table_5 <- dbGetQuery(mydb, "SELECT 
                                    major_label_vansic.major_label_vansic_desc,
                                    ref_yearDetails.tot_paid_emp AS Num_Of_Paid_Employees,
                                    reference_year.reference_year_desc AS Reference_Year,
                                    ref_yearDetails.sales_goods_services AS Total_Sales
                      
                      FROM 
                      
                      main
                      
                      INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                      INNER JOIN ref_yearDetails ON ref_yearDetails.id = main.id
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      
                      
                      
                      WHERE reference_year.reference_year_desc = '2019' OR reference_year.reference_year_desc = '2021'
                      
                      ORDER BY ref_yearDetails.sales_goods_services DESC
                      ")
# Table 6: Number of Employees by Citizenship, Sex and Industry in 2019
Table_6 <- dbGetQuery(mydb, "SELECT reference_year.reference_year_desc AS Reference_Year,
                      major_label_vansic.major_label_vansic_desc AS Major_Industry,
                      ref_yearDetails.local_emp AS Number_Of_Local_Employees,
                      ref_yearDetails.foreign_emp AS Number_Of_Foreign_Employees,
                      ref_yearDetails.mang_vanmale AS NiVan_Male_Managerial_Skills,
                      ref_yearDetails.skill_vanmale AS Skilled_NiVan_Male,
                      ref_yearDetails.unskil_vanmale AS Unskilled_NiVan_Male,
                      ref_yearDetails.mang_vanfemale AS NiVan_Female_Managerial_Skills,
                      ref_yearDetails.skill_vanfemale AS Skilled_NiVan_Female
                      
                      FROM 
                      
                      ref_yearDetails
                      
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      INNER JOIN main ON ref_yearDetails.id = main.id
                      INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                      
                      WHERE reference_year.reference_year_desc = '2019'
                      
                      
                      ")

# Table 7: Number of Employees by Citizenship, Sex, and by Industry in 2021
Table_7 <- dbGetQuery(mydb, "SELECT reference_year.reference_year_desc AS Reference_Year,
                      major_label_vansic.major_label_vansic_desc AS Major_Industry,
                      ref_yearDetails.local_emp AS Number_Of_Local_Employees,
                      ref_yearDetails.foreign_emp AS Number_Of_Foreign_Employees,
                      ref_yearDetails.mang_vanmale AS NiVan_Male_Managerial_Skills,
                      ref_yearDetails.skill_vanmale AS Skilled_NiVan_Male,
                      ref_yearDetails.unskil_vanmale AS Unskilled_NiVan_Male,
                      ref_yearDetails.mang_vanfemale AS NiVan_Female_Managerial_Skills,
                      ref_yearDetails.skill_vanfemale AS Skilled_NiVan_Female
                      
                      FROM 
                      
                      ref_yearDetails
                      
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      INNER JOIN main ON ref_yearDetails.id = main.id
                      INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                      
                      WHERE reference_year.reference_year_desc = '2021'
                      
                      ")

# Table 8: Total Compensation of employees according to Industry, Sex and Local and Foreign in 2019
Table_8 <- dbGetQuery(mydb, "SELECT reference_year.reference_year_desc AS Reference_Year,
                      major_label_vansic.major_label_vansic_desc AS Major_Industry,
                      ref_yearDetails.local_emp AS NiVan_Employees, 
                      ref_yearDetails.foreign_emp AS Foreign_Employees, 
                      ref_yearDetails.mang_vanmale AS NiVan_Male_Manager_Skills, 
                      ref_yearDetails.skill_vanmale AS Skilled_NiVan_Male, 
                      ref_yearDetails.unskil_vanmale AS Unskilled_NiVan_Male, 
                      ref_yearDetails.mang_vanfemale AS NiVan_Female_Manager_Skills, 
                      ref_yearDetails.skill_vanfemale AS Skilled_NiVan_Male, 
                      ref_yearDetails.skill_vanfemale AS Skilled_NiVan_Female, 
                      ref_yearDetails.unskil_vanfemale AS Unskilled_NiVan_Female, 
                      ref_yearDetails.mang_formale AS Foreign_Male_Manager_Skills, 
                      ref_yearDetails.skill_formale AS Skilled_Foreign_Male, 
                      ref_yearDetails.unskil_formale AS Unskilled_Foreign_Male, 
                      ref_yearDetails.mang_forfemale AS Skilled_Foreign_Male, 
                      ref_yearDetails.skill_forfemale AS Foreign_Female_Manager_Skills, 
                      ref_yearDetails.unskil_forfemale AS Unskilled_Foreign_Female
                      
                      FROM
                      
                      ref_yearDetails
                      
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      INNER JOIN main ON ref_yearDetails.id = main.id
                      INNER JOIN major_label_vansic on main.major_label_vansic = major_label_vansic.major_label_vansic
                      
                      WHERE reference_year.reference_year_desc = '2019'
                      
                      ")

# Table 9: Total Compensation of employees according to Industry, Sex and Local and Foreign 2021
Table_9 <- dbGetQuery(mydb, "SELECT reference_year.reference_year_desc AS Reference_Year,
                      major_label_vansic.major_label_vansic_desc AS Major_Industry,
                      ref_yearDetails.local_emp AS NiVan_Employees, 
                      ref_yearDetails.foreign_emp AS Foreign_Employees, 
                      ref_yearDetails.mang_vanmale AS NiVan_Male_Manager_Skills, 
                      ref_yearDetails.skill_vanmale AS Skilled_NiVan_Male, 
                      ref_yearDetails.unskil_vanmale AS Unskilled_NiVan_Male, 
                      ref_yearDetails.mang_vanfemale AS NiVan_Female_Manager_Skills, 
                      ref_yearDetails.skill_vanfemale AS Skilled_NiVan_Male, 
                      ref_yearDetails.skill_vanfemale AS Skilled_NiVan_Female, 
                      ref_yearDetails.unskil_vanfemale AS Unskilled_NiVan_Female, 
                      ref_yearDetails.mang_formale AS Foreign_Male_Manager_Skills, 
                      ref_yearDetails.skill_formale AS Skilled_Foreign_Male, 
                      ref_yearDetails.unskil_formale AS Unskilled_Foreign_Male, 
                      ref_yearDetails.mang_forfemale AS Skilled_Foreign_Male, 
                      ref_yearDetails.skill_forfemale AS Foreign_Female_Manager_Skills, 
                      ref_yearDetails.unskil_forfemale AS Unskilled_Foreign_Female
                      
                      FROM
                      
                      ref_yearDetails
                      
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      INNER JOIN main ON ref_yearDetails.id = main.id
                      INNER JOIN major_label_vansic on main.major_label_vansic = major_label_vansic.major_label_vansic
                      
                      WHERE reference_year.reference_year_desc = '2021'
                      
                      ")

# Table 10: Total Number of Employees by skill type in 2019 and 2021 by Industry
Table_10 <- dbGetQuery(mydb, "SELECT reference_year.reference_year_desc AS Reference_Year,
                      major_label_vansic.major_label_vansic_desc,
                      ref_yearDetails.mang_vanmale, 
                      ref_yearDetails.skill_vanmale, 
                      ref_yearDetails.unskil_vanmale, 
                      ref_yearDetails.mang_vanfemale, 
                      ref_yearDetails.skill_vanfemale, 
                      ref_yearDetails.skill_vanfemale, 
                      ref_yearDetails.unskil_vanfemale, 
                      ref_yearDetails.mang_formale, 
                      ref_yearDetails.skill_formale, 
                      ref_yearDetails.unskil_formale, 
                      ref_yearDetails.mang_forfemale, 
                      ref_yearDetails.skill_forfemale, 
                      ref_yearDetails.unskil_forfemale
                      
                      FROM 
                      
                      ref_yearDetails
                      
                      INNER JOIN reference_year ON ref_yearDetails.ref_yearDetails__id = reference_year.reference_year
                      INNER JOIN main ON ref_yearDetails.id = main.id
                      INNER JOIN major_label_vansic ON main.major_label_vansic = major_label_vansic.major_label_vansic
                      
                      GROUP BY major_label_vansic.major_label_vansic
                      
")

##### Nimal's Data ####
pilotData <- dbGetQuery(mydb, "SELECT main.name_businesEsta AS NAME,
                        main.busines_start_for AS YEAR,
                        main.business_locate AS ESTAB_NO,
                        area_council.area_council_desc AS LOCATION,
                        institution_reg.institution_reg_desc AS Registration,
                        legal_status.legal_status_desc AS LEGALSTAT,
                        account_keeping.account_keeping_desc AS ACCOUNTS
                        
                        FROM 
                        
                        main
                        
                        INNER JOIN area_council ON main.area_council = area_council.area_council
                        INNER JOIN register_id ON main.id = register_id.id
                        INNER JOIN institution_reg ON register_id.register_id__id = institution_reg.institution_reg
                        INNER JOIN legal_status ON main.legal_status = legal_status.legal_status
                        INNER JOIN account_keeping ON main.account_keeping = account_keeping.account_keeping
                        
                        GROUP BY main.name_businesEsta
                        " )
  
#province <- read.csv("data/open/province.csv")

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
