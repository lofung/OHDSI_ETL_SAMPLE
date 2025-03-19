import os
from datetime import datetime
from termcolor import colored

##import from another folder
import sys
sys.path.insert(0, '..')
from tools_and_tests.functions_OK import icd9_dict_generate, icpc_dict_generate, visit_occurrence_dict,  icd10_death_dict_generate,icd10_addition_dict_generate, load_person_map

os.chdir("D:\\OHDSI-THIN2022\\map\\")
from special_map import thin_constype_map, thin_load_map

print(str(datetime.now().time()) + ' script started.')
print(str(datetime.now().time()) + ' loading tables...')

file_list = ["medical.csv"]
source_folder = "Z:\\Cohort Raw Data (do not edit)\\IMRD data THIN2022\\raw csv txt\\data\\"
destination_folder = 'D:\\OHDSI-THIN2022\\for upload\\'
map_folder = 'D:\\OHDSI-THIN2022\\map\\'
table_name = "procedure"
error_list = []
map_missing_list = []
pt_missing_list = []
k = 0

## load person map for this OMOP STUDY
## because all the pt ids are in HASH
## damn cannot just use the codes!
pt_map = load_person_map(map_folder + "pt_code.txt")
#print(pt_map)
#for item in pt_map:
#    print(item, pt_map(item))
print(colored(" " + str(datetime.now().time()) + ' loading patient map... done', "green"))

##load the maps
table_map = thin_load_map("D:\\OHDSI-THIN2022\\export from thin2021\\old_" + table_name + "_map.txt", table_name, "|")

print(colored(str(datetime.now().time()) + ' map loading completed...', "green"))

######### WORK FOR ICD9ip
with open(destination_folder + table_name + '.txt', 'w+',
          encoding="mbcs") as target:
    for file in file_list:
        with open(source_folder + file, 'r', encoding="mbcs") as source:
            next(source)
            for line in source:
                # print(line)
                items = line.replace("\n", "").replace("\"", "").split("|")
                k = k + 1
                table_id = str(k)

                patid = items[0]
                # datatype = items[2] ##likely not useful
                medcode = items[3]  ##THIS IS THE MEDICINE CODE
                # medflag = items[4]
                # staffid = items[5]
                # source_column = items[6]
                # episode = items[7]
                # nhsspec = items[8]
                # locate = items[9]
                # textid = items[10]
                # category = items[11]
                # priority = items[12]
                # medinfo = items[13]
                # inprac = items[14]
                # private = items[15]
                # medid = items[16]
                cnsultid = items[17]
                # modified = items[18]
                evntdate = items[19]
                # dteflag = items[20]
                # sysdate = items[21]
                pracid = items[22]

                try:
                    table_concept_array = table_map[medcode]
                except:
                    continue

                ## pt mapped dictionary
                person_id = pt_map[pracid + "-" + patid]

                event_OMOP_id = table_concept_array["ohdsi-conecpt_id"]
                event_OHDSI_original_id = table_concept_array["source_concept"]

                temp_start_date = evntdate.split("/")
                # print(prscdate)
                try:
                    event_date = temp_start_date[2] + "-" + temp_start_date[1] + "-" + \
                                               temp_start_date[0]
                except:
                    #print(k)
                    #print(evntdate)
                    event_date = ""

                ## load visit occurrence id from map
                visit_occurrence_source_id = pracid + "@" + patid + "@" + cnsultid
                ##line to write into file for upload ONLY if line is not empty
                result_line = "|".join((
                    table_id,  ## procedure occurrence_id
                    person_id,  ## person_id
                    event_OMOP_id,  ##condition_concept_id
                    event_date,  ## procedure_date
                    "",  ## procedure_datetime
                    str(32817),  # 32817 is EHR, #procedure_type_concept_id
                    str(0),  # modifier_concept_id, don't understand this field
                    str(1),
                    # quantity;; If a Procedure has a quantity of ‘0’ in the source, this should default to ‘1’ in the ETL. If there is a record in the source it can be assumed the exposure occurred at least once
                    "",  # provider_id
                    "", #visit_occurrence_id
                    "",  # visit_detail_id
                    medcode,  ##procedure_source_value
                    event_OHDSI_original_id,  ##procedure_source_concept_id
                    "0",  # modifier source value
                    visit_occurrence_source_id + "\n"
                ))

                target.write(result_line)

        print(colored(str(datetime.now().time()) + " " + file + ' loading done...', "green"))

    file_list = ["ahd.csv"]
    for file in file_list:
        with open(source_folder + file, 'r', encoding="mbcs") as source:
            next(source)
            for line in source:
                # print(line)
                items = line.replace("\n", "").replace("\"", "").split("|")
                k = k + 1
                table_id = str(k)


                patid = items[0]
                #ahdcode = items[1]
                #ahdflag = items[2]
                ####data1 = items[3]
                ####data2 = items[4]
                ####data3 = items[5]
                ####data4 = items[6]
                ####data5 = items[7]
                ####data6 = items[8]
                ####medcode = items[9]
                #source = items[10]
                #nhsspec = items[11]
                #locate = items[12]
                #staffid = items[13]
                #textid = items[14]
                #category = items[15]
                #ahdinfo = items[16]
                #inprac = items[17]
                #private = items[18]
                #ahdid = items[19]
                cnsultid = items[20]
                #modified = items[21]
                evntdate = items[22]
                #dteflag = items[23]
                #sysdate = items[24]
                pracid = items[25]

                code_list = [3, 4, 5, 6, 7, 8, 9]

                for code_number in code_list:
                    k = k + 1
                    table_id = str(k)

                    try:
                        table_concept_array = table_map[items[code_number]]
                    except:
                        continue

                    ## pt mapped dictionary
                    person_id = pt_map[pracid + "-" + patid]

                    event_OMOP_id = table_concept_array["ohdsi-conecpt_id"]
                    event_OHDSI_original_id = table_concept_array["source_concept"]

                    temp_start_date = evntdate.split("/")
                    # print(prscdate)
                    try:
                        event_date = temp_start_date[2] + "-" + temp_start_date[1] + "-" + temp_start_date[0]
                    except:
                        #print(k)
                        #print(evntdate)
                        event_date = ""

                    ## load visit occurrence id from map
                    visit_occurrence_source_id = pracid + "@" + patid + "@" + cnsultid
                    ##line to write into file for upload ONLY if line is not empty
                    result_line = "|".join((
                        table_id,  ## procedure occurrence_id
                        person_id,  ## person_id
                        event_OMOP_id,  ##condition_concept_id
                        event_date,  ## procedure_date
                        "",  ## procedure_datetime
                        str(32817),  # 32817 is EHR, #procedure_type_concept_id
                        str(0),  # modifier_concept_id, don't understand this field
                        str(1),
                        # quantity;; If a Procedure has a quantity of ‘0’ in the source, this should default to ‘1’ in the ETL. If there is a record in the source it can be assumed the exposure occurred at least once
                        "",  # provider_id
                        "", #visit_occurrence_id
                        "",  # visit_detail_id
                        medcode,  ##procedure_source_value
                        event_OHDSI_original_id,  ##procedure_source_concept_id
                        "0",  # modifier source value
                        visit_occurrence_source_id + "\n"
                    ))

                    target.write(result_line)

        print(colored(str(datetime.now().time()) + " " + file + ' loading done...', "green"))
