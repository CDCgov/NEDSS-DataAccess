import pyodbc
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.sql import text
import re
import sys
import os
from faker import Faker
import random
from decouple import config
from datetime import datetime

    #if __name__ == "__main__":
    #    numoELRs = int(sys.argv[1])
    #    conditionCode = str(sys.argv[2])
    #
    #    print ("Generating HL7messages for Disease code:", format(conditionCode))

numoELRs = int(sys.argv[1])
conditionCode = str(sys.argv[2])

host = ''
user = ''
password = ''
database = ''

connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+host+";DATABASE="+database+";UID="+user+";PWD="+password
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
from sqlalchemy import create_engine
engine = create_engine(connection_url)

def generateELR(numoELRs, conditionCode, output_folder):
    # Generating values for PID fields and sub-fields
    os.makedirs(output_folder, exist_ok=True)
    for i in range(int(numoELRs)):
        #if conditionCode== '10101':
        
        curr_time = datetime.now()
        #print("Starttime", start_time)

        curr_date=curr_time.strftime("%Y%m%d")

        # Initializing Faker 
        fake = Faker()

        # Generating Patient details using Faker
        patID = fake.random_int(min = 100000000, max = 999999999)
        firstname = fake.first_name()
        lastname = fake.last_name()
        fullname = firstname + " " + lastname
        dob = fake.date_of_birth()
        sex = ["M", "F", "O", "U"]
        patSex = random.choice(sex)
        mails= ['gmail.com', 'hotmail.com', 'yahoo.com', 'icloud.com']
        numberrn = str(random.randint(10, 99))
        email = firstname + lastname + numberrn + "@" + random.choice(mails)
        phone = fake.phone_number()
        ssn = fake.ssn()

        # Generating Address using Faker
        address = fake.street_address()
        building_number = fake.building_number()
        city = fake.city()
        state_abbr = fake.state_abbr()
        zip_code = fake.zipcode()
        country = fake.country()
        #county = fake.county()

        # Lab Report
        time = datetime.now()
        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # ---- Patient Race -----        
        racesql = """select concat (code, ' : ', code_desc_txt) from nbs_srte.dbo.Race_code;"""

        df= pd.read_sql(racesql, engine)
        race_random_row = df.sample(n=1)
        race = race_random_row.to_string(index=False)

        patRace = re.sub("^[^_]* : ", "", race)
        patRaceCode = re.sub(" : [^_]*", "", race).lstrip()

        # ----- Assigning Authority/Facility ------
        assigning_authority = """select concat(eid.root_extension_txt, ' : ', o.display_nm)
                                from nbs_odse..Organization o
                                inner join nbs_odse..entity_id eid
                                on eid.entity_uid = o.organization_uid
                                inner join [nbs_odse].[dbo].[Organization_name] org
                                on o.organization_uid = org.organization_uid
                                where cd = 'LAB' and standard_industry_class_cd = 'CLIA';"""
        df= pd.read_sql(assigning_authority, engine)
        auth_random_row = df.sample(n=1)
        authority = auth_random_row.to_string(index=False)

        assigning_authority_txt = re.sub("^[^_]* : ", "", authority)
        assigning_authority_id = re.sub(" : [^_]*", "", authority).lstrip()

        discode = str(conditionCode)

        # ----- Disese/Condition Code ------

        programArea = "SELECT concat (loc.loinc_cd, ' : ', loc.component_name) FROM nbs_srte..loinc_code loc inner join  nbs_srte..loinc_condition locd ON loc.loinc_cd = locd.loinc_cd inner join nbs_srte..condition_code cond ON locd.condition_cd = cond.condition_cd where locd.condition_cd = '" + discode +"';"

        df= pd.read_sql(programArea, engine)
        disease_random_row = df.sample(n=1)
        disease = disease_random_row.to_string(index=False)

        patDisease = re.sub("^[^_]* : ", "", disease)
        patDiseaseCode = re.sub(" : [^_]*", "", disease).lstrip()

            # ------Segments (Funcitions) ------

            # Message Header. This segment is a mandatory part of an ORU message, 
            # and contains information about the message sender and receiver, 
            # the date and time that the message was created. 
            # This segment is required.

        msh1 = "|" # MSH.1 - Field Separator -- R
        msh2 = "^~\&amp;" # MSH.2 - Encoding Characters -- R
        
        msh3_1 = assigning_authority_txt # MSH.3.1 - Namespace Id ---- Sending Application
        msh3_2 = assigning_authority_id # MSH.3.2 - Universal Id
        msh3_3 = "CLIA" # MSH.3.3 - Universal Id Type
        msh3 = (f"{msh3_1}^{msh3_2}^{msh3_3}")
        
        msh4_1 = assigning_authority_txt # MSH.4.1 - Namespace Id ---- Sending Facility
        msh4_2 = assigning_authority_id # MSH.4.2 - Universal Id
        msh4_3 = "CLIA" # MSH.4.3 - Universal Id Type
        msh4 = (f"{msh4_1}^{msh4_2}^{msh4_3}")
        
        msh5_1 = "" # MSH.5.1 - Namespace Id ---- Recieving Application
        msh5_2 = "" # MSH.5.2 - Universal Id
        msh5_3 = "" # MSH.5.3 - Universal Id Type
        msh5 = ""
        
        msh6_1 = "" # MSH.6.1 - Namespace Id ---- Recieving Facility 
        msh6_2 = "" # MSH.6.2 - Universal Id
        msh6_3 = "" # MSH.6.3 - Universal Id Type
        msh6 = ""
        
        msh7 = formatted_time # MSH.7.1 - Time ---- R
        #msh7_2 = "" # MSH.7.2 - Degree Of Precision
        
        msh8 = "" # MSH.8 - Security 
        
        msh9_1 = 'ORU' # MSH.9.1 - Message Code ---- R
        msh9_2 = 'R01' # MSH.9.2 - Trigger Event ---- R
        msh9_3 = 'ORU_R01' # MSH.9.3 - Message Structure ---- R
        msh9 = (f"{msh9_1}^{msh9_2}^{msh9_3}")
        
        msh10 = "20060904015834571114A" # MSH.10 - Message Control ID ---- R
        
        msh11_1 = "P" # MSH.11.1 - Processing Id ---- R
        msh11_2 = "T" # MSH.11.2 - Processing Mode ---- R
        msh11 = ""
        
        msh12_1 = "2.5.1" # MSH.12.1 - Version Id
        msh12_2 = "" # MSH.12.2 - Internationalization Code
        msh12_3 = "" # MSH.12.3 - International Version Id
        msh12 = ""
        
        msh13 = "" # MSH.13 - Sequence Number
        msh14 = "" # MSH.14 - Continuation Pointer
        msh15 = "" # MSH.15 - Accept Acknowledgment Type
        msh16 = "" # MSH.16 - Application Acknowledgment Type
        msh17 = "" # MSH.17 - Country Code
        msh18 = "" # MSH.18 - Character Set
        msh19_1 = "" # MSH.19.1 - Identifier ----- Principal Language Of Message
        msh19_2 = "" # MSH.19.2 - Text
        msh19_3 = "" # MSH.19.3 - Name Of Coding System
        msh19_4 = "" # MSH.19.4 - Alternate Identifier
        msh19_5 = "" # MSH.19.5 - Alternate Text
        msh19_6 = "" # MSH.19.6 - Name Of Alternate Coding System
        msh19 = ""
        msh20 = "" #MSH20 - Alternate Character Set Handling Scheme
        msh21_1 = "" # MSH.21.1 - Entity Identifier ----- Message Profile Identifier
        msh21_2 = "" # MSH.21.2 - Namespace Id
        msh21_3 = "" # MSH.21.3 - Universal Id
        msh21_4 = "" # MSH.21.4 - Universal Id Type
        msh21 = ""

        MSH = (
        f"MSH|"
        f"{msh1}|{msh2}|{msh3}|{msh4}|{msh5}|{msh6}|{msh7}|{msh8}|{msh9}|{msh10}|"
        f"{msh11}|{msh12}|{msh13}|{msh14}|{msh15}|{msh16}|{msh17}|{msh18}|{msh19}|{msh20}|{msh21}")

        #This segment is used by all applications as the primary means of communicating patient identification information.
        #This segment contains permanent patient identifying and demographic information that, for the most part, is not likely to change frequently.

        ## PID.1 - Set ID -PID
        pid1 = "1"  # PID.1 - Set ID - PID
        ## PID.2 - Patient ID
        pid2_1 = patID  # PID.2.1 - Id Number
        pid2_2 = ""  # PID.2.2 - Check Digit
        pid2_3 = ""  # PID.2.3 - Check Digit Scheme
        pid2_4_1 = assigning_authority_txt # PID 2.4.1 - Namespace Id
        pid2_4_2 = assigning_authority_id # PID 2.4.2 - Universal Id
        pid2_4_3 = "CLIA" # PID 2.4.3 - Universal Id Type
        pid2_4 = (f"{pid2_4_1}~{pid2_4_2}~{pid2_4_3}")  # PID.2.4 - Assigning Authority
        pid2_5 = "U"  # PID.2.5 - Identifier Type Code
        pid2_6 = ""  # PID.2.6 - Assigning Facility
        pid2_6_1 = "" # PID 2.6.1 - Namespace Id
        pid2_6_2 = "" # PID 2.6.2 - Universal Id
        pid2_6_3 = "" # PID 2.6.3 - Universal Id Type
        pid2_7 = ""  # PID.2.7 - Effective Date
        pid2_8 = ""  # PID.2.8 - Expiration Date
        pid2_9 = ""  # PID.2.9 - Assigning Jurisdiction
        pid2_9_1 = "" # PID 2.9.1 - Identifier
        pid2_9_2 = "" # PID 2.9.2 - Text
        pid2_9_3 = "" # PID 2.9.3 - Name of Coading System
        pid2_9_4 = "" # PID 2.9.4 - Alternate Identifier
        pid2_9_5 = "" # PID 2.9.5 - Alternate Text
        pid2_9_6 = "" # PID 2.9.6 - Name Of Alternate Coding System
        pid2_9_7 = "" # PID 2.9.7 - Coding System Version Id
        pid2_9_8 = "" # PID 2.9.8 - Alternate Coding System Version Id
        pid2_9_9 = "" # PID 2.9.9 - Original Text
        pid2_10 = ""  # PID.2.10 - Assigning Agency Or Department
        pid2_10_1 = "" # PID 2.10.1 - Identifier
        pid2_10_2 = "" # PID 2.10.2 - Text
        pid2_10_3 = "" # PID 2.10.3 - Name of Coading System
        pid2_10_4 = "" # PID 2.10.4 - Alternate Identifier
        pid2_10_5 = "" # PID 2.10.5 - Alternate Text
        pid2_10_6 = "" # PID 2.10.6 - Name Of Alternate Coding System
        pid2_10_7 = "" # PID 2.10.7 - Coding System Version Id
        pid2_10_8 = "" # PID 2.10.8 - Alternate Coding System Version Id
        pid2_10_9 = "" # PID 2.10.9 - Original Text
        #pid2 = ("{pid2_1}^{pid2_2}^{pid2_3}^{pid2_4}^{pid2_5}^{pid2_6}^{pid2_7}^{pid2_8}^{pid2_9}^{pid2_10}")
        pid2 = ""
        ## PID.3 - Patient Identifier List
        pid3_1 = patID  # PID.3.1 - Id Number
        pid3_2 = ""  # PID.3.2 - Check Digit
        pid3_3 = ""  # PID.3.3 - Check Digit Scheme
        pid3_4_1 = assigning_authority_txt # PID 3.4.1 - Namespace Id
        pid3_4_2 = assigning_authority_id # PID 3.4.2 - Universal Id
        pid3_4_3 = "CLIA" # PID 3.4.3 - Universal Id Type
        pid3_4 = (f"{pid3_4_1}~{pid3_4_2}~{pid3_4_3}")  # PID.3.4 - Assigning Authority
        pid3_5 = "U"  # PID.3.5 - Identifier Type Code
        pid3_6_1 = "pid3_6_1" # PID 3.6.1 - Namespace Id
        pid3_6_2 = "pid3_6_2" # PID 3.6.2 - Universal Id
        pid3_6_3 = "pid3_6_3" # PID 3.6.3 - Universal Id Type
        #pid3_6 = ("{pid3_6_1}~{pid3_6_2}~{pid3_6_3}")  # PID.3.6 - Assigning Facility
        pid3_6 = ""
        pid3_7 = ""  # PID.3.7 - Effective Date
        pid3_8 = ""  # PID.3.8 - Expiration Date
        pid3_9 = ""  # PID.3.9 - Assigning Jurisdiction
        #pid3_9_1 = "pid3_9_1" # PID 3.9.1 - Identifier
        #pid3_9_2 = "pid3_9_2" # PID 3.9.2 - Text
        #pid3_9_3 = "pid3_9_3" # PID 3.9.3 - Name of Coading System
        #pid3_9_4 = "pid3_9_4" # PID 3.9.4 - Alternate Identifier
        #pid3_9_5 = "pid3_9_5" # PID 3.9.5 - Alternate Text
        #pid3_9_6 = "pid3_9_6" # PID 3.9.6 - Name Of Alternate Coding System
        #pid3_9_7 = "pid3_9_7" # PID 3.9.7 - Coding System Version Id
        #pid3_9_8 = "pid3_9_8" # PID 3.9.8 - Alternate Coding System Version Id
        #pid3_9_9 = "pid3_9_9" # PID 3.9.9 - Original Text
        pid3_10 = ""  # PID.3.10 - Assigning Agency Or Department
        #pid3_10_1 = "pid3_10_1" # PID 3.10.1 - Identifier
        #pid3_10_2 = "pid3_10_2" # PID 3.10.2 - Text
        #pid3_10_3 = "pid3_10_3" # PID 3.10.3 - Name of Coading System
        #pid3_10_4 = "pid3_10_4" # PID 3.10.4 - Alternate Identifier
        #pid3_10_5 = "pid3_10_5" # PID 3.10.5 - Alternate Text
        #pid3_10_6 = "pid3_10_6" # PID 3.10.6 - Name Of Alternate Coding System
        #pid3_10_7 = "pid3_10_7" # PID 3.10.7 - Coding System Version Id
        #pid3_10_8 = "pid3_10_8" # PID 3.10.8 - Alternate Coding System Version Id
        #pid3_10_9 = "pid3_10_9" # PID 3.10.9 - Original Text
        pid3 = (f"{pid3_1}^{pid3_2}^{pid3_3}^{pid3_4}^{pid3_5}^{pid3_6}^{pid3_7}^{pid3_8}^{pid3_9}^{pid3_10}")
        ## PID.4 - Alternate Patient ID
        pid4_1 = "pid4_1"  # PID.4.1 - Id Number
        pid4_2 = "pid4_2"  # PID.4.2 - Check Digit
        pid4_3 = "pid4_3"  # PID.4.3 - Check Digit Scheme
        pid4_4 = "pid4_4"  # PID.4.4 - Assigning Authority
        pid4_5 = "pid4_5"  # PID.4.5 - Identifier Type Code
        pid4_6 = "pid4_6" # PID.4.6 - Assigning Facility
        pid4_7 = "pid4_7" # PID.4.7 - Effective Date
        pid4_8 = "pid4_8" # PID.4.8 - Expiration Date
        pid4_9 = "pid4_9" # PID.4.9 - Assigning Jurisdiction
        pid4_10 = "pid4_10" # PID.4.10 - Assigning Agency Or Department
        #pid4 = ("{pid4_1}^{pid4_2}^{pid4_3}^{pid4_4}^{pid4_5}^{pid4_6}^{pid4_7}^{pid4_8}^{pid4_9}^{pid4_10}")
        pid4 = ""
        ## PID.5 - Patient Name
        pid5_1 = lastname  # PID.5.1 - Family Name
        pid5_2 = firstname  # PID.5.2 - Given Name
        pid5_3 = "SIM_TEST"  # PID.5.3 - Second And Further Given Names Or Initials Thereof
        pid5_4 = ""  # PID.5.4 - Suffix (e.g., Jr Or Iii)
        pid5_5 = ""  # PID.5.5 - Prefix (e.g., Dr)
        pid5_6 = ""  # PID.5.6 - Degree (e.g., Md)
        pid5_7 = ""  # PID.5.7 - Name Type Code
        pid5_8 = ""  # PID.5.8 - Name Representation Code
        pid5_9 = ""  # PID.5.9 - Name Context
        pid5_10 = ""  # PID.5.10 - Name Validity Range
        pid5_11 = ""  # PID.5.11 - Name Assembly Order
        pid5_12 = ""  # PID.5.12 - Effective Date
        pid5_13 = ""  # PID.5.13 - Expiration Date
        pid5_14 = ""  # PID.5.14 - Professional Suffix
        pid5 = (f"{pid5_1}^{pid5_2}^{pid5_3}^{pid5_4}^{pid5_5}^{pid5_6}^{pid5_7}^{pid5_8}^{pid5_9}^{pid5_10}^{pid5_11}^{pid5_12}^{pid5_13}^{pid5_14}")
        ## PID.6 - Mother's Maiden Name
        pid6_1 = "pid6_1"  # PID.6.1 - Family Name
        pid6_2 = "pid6_2"  # PID.6.2 - Given Name
        pid6_3 = "pid6_3"  # PID.6.3 - Second And Further Given Names Or Initials Thereof
        pid6_4 = "pid6_4"  # PID.6.4 - Suffix (e.g., Jr Or Iii)
        pid6_5 = "pid6_5"  # PID.6.5 - Prefix (e.g., Dr)
        pid6_6 = "pid6_6"  # PID.6.6 - Degree (e.g., Md)
        pid6_7 = "pid6_7"  # PID.6.7 - Name Type Code
        pid6_8 = "pid6_8"  # PID.6.8 - Name Representation Code
        pid6_9 = "pid6_9"  # PID.6.9 - Name Context
        pid6_10 = "pid6_10"  # PID.6.10 - Name Validity Range
        pid6_11 = "pid6_11"  # PID.6.11 - Name Assembly Order
        pid6_12 = "pid6_12"  # PID.6.12 - Effective Date
        pid6_13 = "pid6_13"  # PID.6.13 - Expiration Date
        pid6_14 = "pid6_14"  # PID.6.14 - Professional Suffix
        #pid6 = ("{pid6_1}^{pid6_2}^{pid6_3}^{pid6_4}^{pid6_5}^{pid6_6}^{pid6_7}^{pid6_8}^{pid6_9}^{pid6_10}^{pid6_11}^{pid6_12}^{pid6_13}^{pid6_14}")
        pid6 = ""
        ## PID.7 - Date/Time of Birth
        pid7 = dob #<yyyymmdd>
        ## PID.8 - Administrative Sex
        pid8 = patSex
        ## PID.9 - Patient Alias
        pid9_1 = "pid9_1"  # PID.9.1 - Family Name
        pid9_2 = "pid9_2"  # PID.9.2 - Given Name
        pid9_3 = "pid9_3"  # PID.9.3 - Second And Further Given Names Or Initials Thereof
        pid9_4 = "pid9_4"  # PID.9.4 - Suffix (e.g., Jr Or Iii)
        pid9_5 = "pid9_5"  # PID.9.5 - Prefix (e.g., Dr)
        pid9_6 = "pid9_6"  # PID.9.6 - Degree (e.g., Md)
        pid9_7 = "pid9_7"  # PID.9.7 - Name Type Code
        pid9_8 = "pid9_8"  # PID.9.8 - Name Representation Code
        pid9_9 = "pid9_9"  # PID.9.9 - Name Context
        pid9_10 = "pid9_10"  # PID.9.10 - Name Validity Range
        pid9_11 = "pid9_11"  # PID.9.11 - Name Assembly Order
        pid9_12 = "pid9_12"  # PID.9.12 - Effective Date
        pid9_13 = "pid9_13"  # PID.9.13 - Expiration Date
        pid9_14 = "pid9_14"  # PID.9.14 - Professional Suffix
        #pid9 = ("{pid9_1}^{pid9_2}^{pid9_3}^{pid9_4}^{pid9_5}^{pid9_6}^{pid9_7}^{pid9_8}^{pid9_9}^{pid9_10}^{pid9_11}^{pid9_12}^{pid9_13}^{pid9_14}")
        pid9 = ""
        ## PID.10 - Race
        pid10_1 = patRaceCode  # PID.10.1 - Identifier
        pid10_2 = patRace  # PID.10.2 - Text
        pid10_3 = ""  # PID.10.3 - Name Of Coding System
        pid10_4 = ""  # PID.10.4 - Alternate Identifier
        pid10_5 = "SIM_TEST"  # PID.10.5 - Alternate Text
        pid10_6 = ""  # PID.10.6 - Name Of Alternate Coding System
        pid10 = (f"{pid10_1}^{pid10_2}^{pid10_3}^{pid10_4}^{pid10_5}^{pid10_6}")
        ## PID.11 - Patient Address
        pid11_1 = address  # PID.11.1 - Street Address
        pid11_2 = ""   # PID.11.2 - Other Designation
        pid11_3 = city  # PID.11.3 - City
        pid11_4 = state_abbr  # PID.11.4 - State Or Province
        pid11_5 = zip_code  # PID.11.5 - Zip Or Postal Code
        pid11_6 = country  # PID.11.6 - Country
        pid11_7 = "SIM_TEST"  # PID.11.7 - Address Type
        pid11_8 = ""  # PID.11.8 - Other Geographic Designation
        pid11_9 = ""  # PID.11.9 - County/Parish Code
        pid11_10 = ""  # PID.11.10 - Census Tract
        pid11_11 = ""  # PID.11.11 - Address Representation Code
        pid11_12 = ""  # PID.11.12 - Address Validity Range
        pid11_13 = ""  # PID.11.13 - Effective Date
        pid11_14 = ""  # PID.11.14 - Expiration Date
        pid11 = (f"{pid11_1}^{pid11_2}^{pid11_3}^{pid11_4}^{pid11_5}^{pid11_6}^{pid11_7}^{pid11_8}^{pid11_9}^{pid11_10}^{pid11_11}^{pid11_12}^{pid11_13}^{pid11_14}")
        ## PID.12 - County Code
        pid12 = ""
        ## PID.13 - Phone Number - Home
        pid13_1 = phone  # PID.13.1 - Telephone Number
        pid13_2 = ""  # PID.13.2 - Telecommunication Use Code
        pid13_3 = ""  # PID.13.3 - Telecommunication Equipment Type
        pid13_4 = email  # PID.13.4 - Email Address
        pid13_5 = ""  # PID.13.5 - Country Code
        pid13_6 = ""  # PID.13.6 - Area/City Code
        pid13_7 = ""  # PID.13.7 - Local Number
        pid13_8 = ""  # PID.13.8 - Extension
        pid13_9 = ""  # PID.13.9 - Any Text
        pid13_10 = ""  # PID.13.10 - Extension Prefix
        pid13_11 = ""  # PID.13.11 - Speed Dial Code
        pid13_12 = ""  # PID.13.12 - Unformatted Telephone Number
        pid13 = (f"{pid13_1}^{pid13_2}^{pid13_3}^{pid13_4}^{pid13_5}^{pid13_6}^{pid13_7}^{pid13_8}^{pid13_9}^{pid13_10}^{pid13_11}^{pid13_12}")
        ## PID.14 - Phone Number - Business
        pid14_1 = "pid14_1"  # PID.14.1 - Telephone Number
        pid14_2 = "pid14_2"  # PID.14.2 - Telecommunication Use Code
        pid14_3 = "pid14_3"  # PID.14.3 - Telecommunication Equipment Type
        pid14_4 = "pid14_4"  # PID.14.4 - Email Address
        pid14_5 = "pid14_5"  # PID.14.5 - Country Code
        pid14_6 = "pid14_6"  # PID.14.6 - Area/City Code
        pid14_7 = "pid14_7"  # PID.14.7 - Local Number
        pid14_8 = "pid14_8"  # PID.14.8 - Extension
        pid14_9 = "pid14_9"  # PID.14.9 - Any Text
        pid14_10 = "pid14_10"  # PID.14.10 - Extension Prefix
        pid14_11 = "pid14_11"  # PID.14.11 - Speed Dial Code
        pid14_12 = "pid14_12"  # PID.14.12 - Unformatted Telephone Number
        #pid14 = ("{pid14_1}^{pid14_2}^{pid14_3}^{pid14_4}^{pid14_5}^{pid14_6}^{pid14_7}^{pid14_8}^{pid14_9}^{pid14_10}^{pid14_11}^{pid14_12}")
        pid14 = ""
        ## PID.15 - Primary Language
        pid15_1 = "pid15_1"  # PID.15.1 - Identifier
        pid15_2 = "pid15_2"  # PID.15.2 - Text
        pid15_3 = "pid15_3"  # PID.15.3 - Name Of Coding System
        pid15_4 = "pid15_4"  # PID.15.4 - Alternate Identifier
        pid15_5 = "pid15_5"  # PID.15.5 - Alternate Text
        pid15_6 = "pid15_6"  # PID.15.6 - Name Of Alternate Coding System
        #pid15 = ("{pid15e1}^{pid15_2}^{pid15_3}^{pid15_4}^{pid15_5}^{pid15_6}")
        pid15 = ""
        ## PID.16 - Marital Status
        pid16_1 = "T"  # PID.16.1 - Identifier
        pid16_2 = "SIM_TEST"  # PID.16.2 - Text
        pid16_3 = ""  # PID.16.3 - Name Of Coding System
        pid16_4 = ""  # PID.16.4 - Alternate Identifier
        pid16_5 = ""  # PID.16.5 - Alternate Text
        pid16_6 = ""  # PID.16.6 - Name Of Alternate Coding System
        pid16 = (f"{pid16_1}^{pid16_2}^{pid16_3}^{pid16_4}^{pid16_5}^{pid16_6}")
        ## PID.17 - Religion
        pid17_1 = "pid17_1"  # PID.17.1 - Identifier
        pid17_2 = "pid17_2"  # PID.17.2 - Text
        pid17_3 = "pid17_3"  # PID.17.3 - Name Of Coding System
        pid17_4 = "pid17_4"  # PID.17.4 - Alternate Identifier
        pid17_5 = "pid17_5"  # PID.17.5 - Alternate Text
        pid17_6 = "pid17_6"  # PID.17.6 - Name Of Alternate Coding System
        #pid17 = ("{pid17_1}^{pid17_2}^{pid17_3}^{pid17_4}^{pid17_5}^{pid17_6}")
        pid17 = ""
        ## PID.18 - Patient Account Number
        pid18_1 = "pid18_1"  # PID.18.1 - Id Number
        pid18_2 = "pid18_2"  # PID.18.2 - Check Digit
        pid18_3 = "pid18_3"  # PID.18.3 - Check Digit Scheme
        pid18_4 = "pid18_4"  # PID.18.4 - Assigning Authority
        pid18_5 = "pid18_5"  # PID.18.5 - Identifier Type Code
        pid18_6 = "pid18_6"  # PID.18.6 - Assigning Facility
        pid18_7 = "pid18_7"  # PID.18.7 - Effective Date
        pid18_8 = "pid18_8"  # PID.18.8 - Expiration Date
        pid18_9 = "pid18_9"  # PID.18.9 - Assigning Jurisdiction
        pid18_10 = "pid18_10"  # PID.18.10 - Assigning Agency Or Department
        #pid18 = ("{pid18_1}^{pid18_2}^{pid18_3}^{pid18_4}^{pid18_5}^{pid18_6}^{pid18_7}^{pid18_8}^{pid18_9}^{pid18_10}")
        pid18 = ""
        ## PID.19 - SSN Number - Patient
        pid19 = ssn
        ## PID.20 - Driver's License Number - Patient
        pid20_1 = "pid20_1"  # PID.20.1 - License Number
        pid20_2 = "pid20_2"  # PID.20.2 - Issuing State, Province, Country
        pid20_3 = "pid20_3"  # PID.20.3 - Expiration Date
        #pid20 = ("{pid20_1}^{pid20_2}^{pid20_3}")
        pid20 = ""
        ##PID.21 - Mother's Identifier
        pid21 = ""
        ##PID.22 - Ethnic Group
        pid22 = ""
        ##PID.23 - Birth Place
        pid23 = ""
        ##PID.24 - Multiple Birth Indicator
        pid24 = ""
        ##PID.25 - Birth Order
        pid25 = ""
        ##PID.26 - Citizenship
        pid26 = ""
        ##PID.27 - Veterans Military Status
        pid27 = ""
        ##PID.28 - Nationality
        pid28 = ""
        ##PID.29 - Patient Death Date and Time
        pid29 = ""
        ##PID.30 - Patient Death Indicator
        pid30 = ""
        ##PID.31 - Identity Unknown Indicator
        pid31 = ""
        ##PID.32 - Identity Reliability Code
        pid32 = ""
        ##PID.33 - Last Update Date/Time
        pid33 = ""
        ##PID.34 - Last Update Facility
        pid34 = ""
        ##PID.35 - Species Code
        pid35 = ""
        ##PID.36 - Breed Code
        pid36 = ""
        ##PID.37 - Strain
        pid37 = ""
        ##PID.38 - Production Class Code
        pid38 = ""
        ##PID.39 - Tribal Citizenship
        pid39 = ""
        
        # Assigning field values into a template
        PID = (
        f"PID|"
        f"{pid1}|{pid2}|{pid3}|{pid4}|{pid5}|{pid6}|{pid7}|{pid8}|{pid9}|{pid10}|"
        f"{pid11}|{pid12}|{pid13}|{pid14}|{pid15}|{pid16}|{pid17}|{pid18}|{pid19}|{pid20}|"
        f"{pid21}|{pid22}|{pid23}|{pid24}|{pid25}|{pid26}|{pid27}|{pid28}|{pid29}|{pid30}|"
        f"{pid31}|{pid32}|{pid33}|{pid34}|{pid35}|{pid36}|{pid37}|{pid38}|{pid39}")
        
        # PID variable is being returned here
        HL7 = (f"{MSH}"
            f"\n{PID}")
        
        # print ("\n This is the HL7 Message for :", patDisease)

        # print(HL7)
        End_time = datetime.now()
        #print("Endtime", End_time)
        
        # Create a separate text file for each message
        file_name = os.path.join(output_folder, f"{firstname}_{lastname}_{patDiseaseCode}_{curr_date}.txt")
        with open(file_name, 'w') as text_file:
            text_file.write(HL7)


generateELR(numoELRs, conditionCode, "/Users/SubbaReddyAlla/Documents/WORK/HL7-Generation/ELR_Generator_File_Drop")