# ---------- HL7 Data Generator
import random
import pyodbc
from faker import Faker
import json
#from dictionary.py import PIDvalueGenerate


# ---------- REQUIREMENTS/GOALS for V1 ---------------

# Generate/Simulate the HL7 messages by disease type/code
# all required fields for v1 (see the included segmants for the version1 below)
# Parameters: diseaseCode, numberOfELRs
# API endpoint to access the HL7 generator service /Jmeter


# ---------- GENERAL REQUIREMENTS/GOALS ------------
# Generate a message with all segments and fields. (pid, nk1, msh, obr, orc, obx)
# Negative testing
#  Simulate pandemic: batch (), 10k per sec
#  Integrate with DI Service to create a data flow
#  Should be updates and not always net new
#  additional Parameters like JDs (?),  
   ## Generate a message excluding specific segments.
   ## Generate a message excluding specific fields.
#  UI to monitor: how many submitted, status update *for NBS

# --------- Segments list for V1: ---------

# Message Header (MSH), 
# Patient Result:
# Patient:
#     Patient Identification (PD1), 
# Visit:
#     Patient Visit (PV1), 
# OrderObservation:
#         Observation Request (OBR)
#     TimingQuantity:
#         Timing/Quantity (TQ1)
#     Observation:
#         Observation/Result (OBX)
#     Specimen:
#         Specimen (SPM)


# ---------- **********Change/Version control********** ----------
# ***Date***    ***Version***     ***User***       ***Changes***

# 01/23/2024          V1              Sneha           Checked in the initial version of the POC code to Github
# 01/30/2024          V1              Subba           Adding Templates to all the required Segments and fields for the Version 1


    


class HL7v2_5_1_ORU:
    def __init__(self, first, last, option):
        self.first = first
        self.last = last
       # if self.option = 'allsegments':
        #    HL7v2_5_1_ORU.allsegments()
        #if self.option = 'invalid':
           # HL7v2_5_1_ORU.invalid()
        #else print('enter a correct option (allsegment, invalid, update, etc.)')
    
    # This segment defines the intent, source, destination, and some specifics of the syntax of a message.
    def MSH():
        # Message Header. This segment is a mandatory part of an ORU message, 
        # and contains information about the message sender and receiver, 
        # the date and time that the message was created. 
        # This segment is required.

        msh1 = "|" # MSH.1 - Field Separator -- R
        msh2 = "^~\&amp;" # MSH.2 - Encoding Characters -- R
        msh3_1 = "msh3_1" # MSH.3.1 - Namespace Id ---- Sending Application
        msh3_2 = "msh3_2" # MSH.3.2 - Universal Id
        msh3_3 = "msh3_3" # MSH.3.3 - Universal Id Type
        msh4_1 = "msh4_1" # MSH.4.1 - Namespace Id ---- Sending Facility
        msh4_2 = msh4_1 # MSH.4.2 - Universal Id
        msh4_3 = msh4_3 # MSH.4.3 - Universal Id Type
        msh5_1 = msh5_1 # MSH.5.1 - Namespace Id ---- Recieving Application
        msh5_2 = msh5_2 # MSH.5.2 - Universal Id
        msh5_3 = msh5_3 # MSH.5.3 - Universal Id Type
        msh6_1 = msh6_1 # MSH.6.1 - Namespace Id ---- Recieving Facility 
        msh6_2 = msh6_2 # MSH.6.2 - Universal Id
        msh6_3 = msh6_3 # MSH.6.3 - Universal Id Type
        msh7_1 = msh7_1 # MSH.7.1 - Time ---- R
        msh7_2 = msh7_2 # MSH.7.2 - Degree Of Precision
        msh8 = msh8 # MSH.8 - Security 
        msh9_1 = msh9_1 # MSH.9.1 - Message Code ---- R
        msh9_2 = msh9_2 # MSH.9.2 - Trigger Event ---- R
        msh9_3 = msh9_3 # MSH.9.3 - Message Structure ---- R
        msh10 = msh10 # MSH.10 - Message Control ID ---- R
        msh11_1 = msh11_1 # MSH.11.1 - Processing Id ---- R
        msh11_2 = msh11_2 # MSH.11.2 - Processing Mode ---- R
        msh12_1 = msh12_1 # MSH.12.1 - Version Id
        msh12_2 = msh12_2 # MSH.12.2 - Internationalization Code
        msh12_3 = msh12_3 # MSH.12.3 - International Version Id
        msh13 = msh13 # MSH.13 - Sequence Number
        msh14 = msh14 # MSH.14 - Continuation Pointer
        msh15 = msh15 # MSH.15 - Accept Acknowledgment Type
        msh16 = msh16 # MSH.16 - Application Acknowledgment Type
        msh17 = msh17 # MSH.17 - Country Code
        msh18 = msh18 # MSH.18 - Character Set
        msh19_1 = msh19_1 # MSH.19.1 - Identifier ----- Principal Language Of Message
        msh19_2 = msh19_2 # MSH.19.2 - Text
        msh19_3 = msh19_3 # MSH.19.3 - Name Of Coding System
        msh19_4 = msh19_4 # MSH.19.4 - Alternate Identifier
        msh19_5 = msh19_5 # MSH.19.5 - Alternate Text
        msh19_6 = msh19_6 # MSH.19.6 - Name Of Alternate Coding System
        msh21_1 = msh21_1 # MSH.21.1 - Entity Identifier ----- Message Profile Identifier
        msh21_2 = msh21_2 # MSH.21.2 - Namespace Id
        msh21_3 = msh21_3 # MSH.21.3 - Universal Id
        msh21_4 = msh21_4 # MSH.21.4 - Universal Id Type

        MSH = ()
        return MSH

    #This segment provides additional information about the software product(s) used as a Sending Application.
    def SFT(self):
        # Optional for V1.0
        pass

    #This segment is used by all applications as the primary means of communicating patient identification information.
    #This segment contains permanent patient identifying and demographic information that, for the most part, is not likely to change frequently.
    def PID():
        # Generating values for PID fields and sub-fields
        
        # Initializing Faker 
        fake = Faker()

        # Generating patient details using Faker
        firstname = fake.first_name()
        lastname = fake.last_name()
        fullname = firstname + " " + lastname
        sex = ["M", "F", "O", "U"]
        mails= ['gmail.com', 'hotmail.com', 'yahoo.com', 'icloud.com']
        numberrn = str(random.randint(10, 99))
        email = firstname + lastname + numberrn + "@" + random.choice(mails)
        race = {"1002-5": "American Indian or Alaska Native",	
                "2028-9":"Asian",
                "2054-5":"Black or African American",
                "2076-8": "Native Hawaiian or Other Pacific Islander",
                "2106-3":"White", "2131-1":"Other Race"}
        patRace = random.choice(race)
        raceCode = list(race.keys())
        patRaceCode = random.choice(raceCode)

        # Generating Address using Faker
        address = fake.street_address()
        building_number = fake.building_number()
        city = fake.city()
        state_abbr = fake.state_abbr()
        zip_code = fake.zipcode()
        country = fake.country()

        # Assigning values to all the PID fields 

        pid1 = "1"  # PID.1 - Set ID - PID
        pid2 = "pid2"  # PID.2 - Patient ID
        pid2_1 = "pid2_1"  # PID.2.1 - Id Number
        pid2_2 = "pid2_2"  # PID.2.2 - Check Digit
        pid2_3 = "pid2_3"  # PID.2.3 - Check Digit Scheme
        pid2_4 = "pid2_4"  # PID.2.4 - Assigning Authority
        pid2_5 = "pid2_5"  # PID.2.5 - Identifier Type Code
        pid2_6 = "pid2_6"  # PID.2.6 - Assigning Facility
        pid2_7 = "pid2_7"  # PID.2.7 - Effective Date
        pid2_8 = "pid2_8"  # PID.2.8 - Expiration Date
        pid2_9 = "pid2_9"  # PID.2.9 - Assigning Jurisdiction
        pid2_10 = "pid2_10"  # PID.2.10 - Assigning Agency Or Department
        pid3 = "pid3"  # PID.3 - Patient Identifier List
        pid3_1 = "pid3_1"  # PID.3.1 - Id Number
        pid3_2 = "pid3_2"  # PID.3.2 - Check Digit
        pid3_3 = "pid3_3"  # PID.3.3 - Check Digit Scheme
        pid3_4 = "pid3_4"  # PID.3.4 - Assigning Authority
        pid3_5 = "pid3_5"  # PID.3.5 - Identifier Type Code
        pid3_6 = "pid3_6"  # PID.3.6 - Assigning Facility
        pid3_7 = "pid3_7"  # PID.3.7 - Effective Date
        pid3_8 = "pid3_8"  # PID.3.8 - Expiration Date
        pid3_9 = "pid3_9"  # PID.3.9 - Assigning Jurisdiction
        pid3_10 = "pid3_10"  # PID.3.10 - Assigning Agency Or Department
        pid4 = "pid4"  # PID.4 - Alternate Patient ID
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
        pid5 = "pid5"  # PID.5 - Patient Name
        pid5_1 = "pid5_1"  # PID.5.1 - Family Name
        pid5_2 = "pid5_2"  # PID.5.2 - Given Name
        pid5_3 = "pid5_3"  # PID.5.3 - Second And Further Given Names Or Initials Thereof
        pid5_4 = "pid5_4"  # PID.5.4 - Suffix (e.g., Jr Or Iii)
        pid5_5 = "pid5_5"  # PID.5.5 - Prefix (e.g., Dr)
        pid5_6 = "pid5_6"  # PID.5.6 - Degree (e.g., Md)
        pid5_7 = "pid5_7"  # PID.5.7 - Name Type Code
        pid5_8 = "pid5_8"  # PID.5.8 - Name Representation Code
        pid5_9 = "pid5_9"  # PID.5.9 - Name Context
        pid5_10 = "pid5_10"  # PID.5.10 - Name Validity Range
        pid5_11 = "pid5_11"  # PID.5.11 - Name Assembly Order
        pid5_12 = "pid5_12"  # PID.5.12 - Effective Date
        pid5_13 = "pid5_13"  # PID.5.13 - Expiration Date
        pid5_14 = "pid5_14"  # PID.5.14 - Professional Suffix
        pid6 = "pid6"  # PID.6 - Mother's Maiden Name
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
        pid7 = "pid7"  # PID.7 - Date/Time of Birth
        pid7_1 = "pid7_1"  # PID.7.1 - Time
        pid7_2 = "pid7_2"  # PID.7.2 - Degree Of Precision
        pid8 = "pid8"  # PID.8 - Administrative Sex
        pid9 = "pid9"  # PID.9 - Patient Alias
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
        pid10 = "pid10"  # PID.10 - Race
        pid10_1 = "pid10_1"  # PID.10.1 - Identifier
        pid10_2 = "pid10_2"  # PID.10.2 - Text
        pid10_3 = "pid10_3"  # PID.10.3 - Name Of Coding System
        pid10_4 = "pid10_4"  # PID.10.4 - Alternate Identifier
        pid10_5 = "pid10_5"  # PID.10.5 - Alternate Text
        pid10_6 = "pid10_6"  # PID.10.6 - Name Of Alternate Coding System
        pid11 = "pid11"  # PID.11 - Patient Address
        pid11_1 = "pid11_1"  # PID.11.1 - Street Address
        pid11_2 = "pid11_2"  # PID.11.2 - Other Designation
        pid11_3 = "pid11_3"  # PID.11.3 - City
        pid11_4 = "pid11_4"  # PID.11.4 - State Or Province
        pid11_5 = "pid11_5"  # PID.11.5 - Zip Or Postal Code
        pid11_6 = "pid11_6"  # PID.11.6 - Country
        pid11_7 = "pid11_7"  # PID.11.7 - Address Type
        pid11_8 = "pid11_8"  # PID.11.8 - Other Geographic Designation
        pid11_9 = "pid11_9"  # PID.11.9 - County/Parish Code
        pid11_10 = "pid11_10"  # PID.11.10 - Census Tract
        pid11_11 = "pid11_11"  # PID.11.11 - Address Representation Code
        pid11_12 = "pid11_12"  # PID.11.12 - Address Validity Range
        pid11_13 = "pid11_13"  # PID.11.13 - Effective Date
        pid11_14 = "pid11_14"  # PID.11.14 - Expiration Date
        pid12 = "pid12"  # PID.12 - County Code
        pid13 = "pid13"  # PID.13 - Phone Number - Home
        pid13_1 = "pid13_1"  # PID.13.1 - Telephone Number
        pid13_2 = "pid13_2"  # PID.13.2 - Telecommunication Use Code
        pid13_3 = "pid13_3"  # PID.13.3 - Telecommunication Equipment Type
        pid13_4 = "pid13_4"  # PID.13.4 - Email Address
        pid13_5 = "pid13_5"  # PID.13.5 - Country Code
        pid13_6 = "pid13_6"  # PID.13.6 - Area/City Code
        pid13_7 = "pid13_7"  # PID.13.7 - Local Number
        pid13_8 = "pid13_8"  # PID.13.8 - Extension
        pid13_9 = "pid13_9"  # PID.13.9 - Any Text
        pid13_10 = "pid13_10"  # PID.13.10 - Extension Prefix
        pid13_11 = "pid13_11"  # PID.13.11 - Speed Dial Code
        pid13_12 = "pid13_12"  # PID.13.12 - Unformatted Telephone Number
        pid14 = "pid14"  # PID.14 - Phone Number - Business
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
        pid15 = "pid15"  # PID.15 - Primary Language
        pid15_1 = "pid15_1"  # PID.15.1 - Identifier
        pid15_2 = "pid15_2"  # PID.15.2 - Text
        pid15_3 = "pid15_3"  # PID.15.3 - Name Of Coding System
        pid15_4 = "pid15_4"  # PID.15.4 - Alternate Identifier
        pid15_5 = "pid15_5"  # PID.15.5 - Alternate Text
        pid15_6 = "pid15_6"  # PID.15.6 - Name Of Alternate Coding System
        pid16 = "pid16"  # PID.16 - Marital Status
        pid16_1 = "pid16_1"  # PID.16.1 - Identifier
        pid16_2 = "pid16_2"  # PID.16.2 - Text
        pid16_3 = "pid16_3"  # PID.16.3 - Name Of Coding System
        pid16_4 = "pid16_4"  # PID.16.4 - Alternate Identifier
        pid16_5 = "pid16_5"  # PID.16.5 - Alternate Text
        pid16_6 = "pid16_6"  # PID.16.6 - Name Of Alternate Coding System
        pid17 = "pid17"  # PID.17 - Religion
        pid17_1 = "pid17_1"  # PID.17.1 - Identifier
        pid17_2 = "pid17_2"  # PID.17.2 - Text
        pid17_3 = "pid17_3"  # PID.17.3 - Name Of Coding System
        pid17_4 = "pid17_4"  # PID.17.4 - Alternate Identifier
        pid17_5 = "pid17_5"  # PID.17.5 - Alternate Text
        pid17_6 = "pid17_6"  # PID.17.6 - Name Of Alternate Coding System
        pid18 = "pid18"  # PID.18 - Patient Account Number
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
        pid19 = "pid19"  # PID.19 - SSN Number - Patient
        pid20 = "pid20"  # PID.20 - Driver's License Number - Patient
        pid20_1 = "pid20_1"  # PID.20.1 - License Number
        pid20_2 = "pid20_2"  # PID.20.2 - Issuing State, Province, Country
        pid20_3 = "pid20_3"  # PID.20.3 - Expiration Date
        pid21 = "pid21"  # PID.21 - Mother's Identifier
        pid22 = "pid22" # PID.22 - Ethnic Group
        pid23 = "pid23" # PID.23 - Birth Place
        pid24 = "pid24" # PID.24 - Multiple Birth Indicator
        pid25 = "pid25" # PID.25 - Birth Order
        pid26 = "pid26" # PID.26 - Citizenship
        pid27 = "pid27" # PID.27 - Veterans Military Status
        pid28 = "pid28" # PID.28 - Nationality
        pid29 = "pid29" # PID.29 - Patient Death Date and Time
        pid30 = "pid30" # PID.30 - Patient Death Indicator
        pid31 = "pid31" # PID.31 - Identity Unknown Indicator
        pid32 = "pid32" # PID.32 - Identity Reliability Code
        pid33 = "pid33" # PID.33 - Last Update Date/Time
        pid34 = "pid34" # PID.34 - Last Update Facility
        pid35 = "pid35" # PID.35 - Species Code
        pid36 = "pid36" # PID.36 - Breed Code
        pid37 = "pid37" # PID.37 - Strain
        pid38 = "pid38" # PID.38 - Production Class Code
        pid39 = "pid39" # PID.39 - Tribal Citizenship
        
        # Assigning field values into a template
        PID = (
        f"PID|"
        f"{pid1}|{pid2_1}^{pid2_2}^{pid2_3}|{pid3}^{pid3_1}^{pid3_2}^{pid3_3}|"
        f"{pid4}^{pid4_1}^{pid4_2}^{pid4_3}^{pid4_4}^{pid4_5}^{pid4_6}^{pid4_7}^{pid4_8}^{pid4_9}^{pid4_10}|"
        f"{pid5}^{pid5_1}^{pid5_2}^{pid5_3}^{pid5_4}^{pid5_5}^{pid5_6}^{pid5_7}^{pid5_8}^{pid5_9}^{pid5_10}^{pid5_11}|"
        f"{pid5_12}^{pid5_13}^{pid5_14}|{pid6}^{pid6_1}^{pid6_2}^{pid6_3}^{pid6_4}^{pid6_5}^{pid6_6}^{pid6_7}^{pid6_8}^{pid6_9}^{pid6_10}^{pid6_11}|"
        f"{pid6_12}^{pid6_13}^{pid6_14}|{pid7}^{pid7_1}^{pid7_2}|{pid8}|{pid9}^{pid9_1}^{pid9_2}^{pid9_3}^{pid9_4}^{pid9_5}^{pid9_6}^{pid9_7}^{pid9_8}^{pid9_9}^{pid9_10}|"
        f"{pid9_11}^{pid9_12}^{pid9_13}^{pid9_14}|{pid10}^{pid10_1}^{pid10_2}^{pid10_3}^{pid10_4}^{pid10_5}^{pid10_6}|{pid11_1}^{pid11_2}^{pid11_3}^{pid11_4}^{pid11_5}|"
        f"{pid11_6}^{pid11_7}^{pid11_8}^{pid11_9}^{pid11_10}^{pid11_11}^{pid11_12}^{pid11_13}^{pid11_14}|{pid12}|{pid13}^{pid13_1}^{pid13_2}^{pid13_3}^{pid13_4}|"
        f"{pid13_5}^{pid13_6}^{pid13_7}^{pid13_8}^{pid13_9}^{pid13_10}^{pid13_11}^{pid13_12}|{pid14}^{pid14_1}^{pid14_2}^{pid14_3}^{pid14_4}^{pid14_5}^{pid14_6}^{pid14_7}|"
        f"{pid14_8}^{pid14_9}^{pid14_10}^{pid14_11}^{pid14_12}|{pid15}^{pid15_1}^{pid15_2}^{pid15_3}^{pid15_4}^{pid15_5}^{pid15_6}|{pid16}^{pid16_1}^{pid16_2}^{pid16_3}^{pid16_4}|"
        f"{pid16_5}^{pid16_6}|{pid17}^{pid17_1}^{pid17_2}^{pid17_3}^{pid17_4}^{pid17_5}^{pid17_6}|{pid18}^{pid18_1}^{pid18_2}^{pid18_3}^{pid18_4}^{pid18_5}^{pid18_6}^{pid18_7}|"
        f"{pid18_8}^{pid18_9}^{pid18_10}|{pid19}|{pid20}^{pid20_1}^{pid20_2}^{pid20_3}|{pid21}|{pid22}|{pid23}|{pid24}|{pid25}|{pid26}|{pid27}|{pid28}|{pid29}|{pid30}|"
        f"{pid31}|{pid32}|{pid33}|{pid34}|{pid35}|{pid36}|{pid37}|{pid38}|{pid39}")
        
        # PID variable is being returned here
        return PID
    
    #This segment contains demographic information that is likely to change about the patient.
    def PD1(self):
        # Optional for V1.0
        # Assigning values to all the PID fields that contain Patient Additional Demographic information    

        # PD1.1 - Living Dependency
        # PD1.2 - Living Arrangement
        # PD1.3 - Patient Primary Facility
        # PD1.3.1 - Organization Name
        pass
    #This segment is commonly used for sending notes and comments.
    def NTE(self):
        # Optional for V1.0
        pass
    #This segment contains information about the patients other related parties. 
    def NK1(self):
        # Optional for V1.0
        pass

    #This segment is used by Registration/Patient Administration applications to communicate information on an account or visit-specific basis.
    def PV1():
        # The PV1 segment is used by Registration/Patient Administration applications to communicate information on an account or visit-specific basis.

        pv1_1 = pv1_1 # PV1.1 - Set ID - PV1
        pv1_2 = pv1_2 # PV1.2 - Patient Class ----- R
        pv1_3 = pv1_3 # PV1.3 - Assigned Patient Location
        pv1_4 = pv1_4 # PV1.4 - Admission Type
        pv1_5 = pv1_5 # PV1.5 - Preadmit Number
        pv1_6 = pv1_6 # PV1.6 - Prior Patient Location
        pv1_7 = pv1_7 # PV1.7 - Attending Doctor
        pv1_8 = pv1_8 # PV1.8 - Referring Doctor
        pv1_9 = pv1_9 # PV1.9 - Consulting Doctor
        pv1_10 = pv1_10 # PV1.10 - Hospital Service
        pv1_11 = pv1_11 # PV1.11 - Temporary Location
        pv1_12 = pv1_12 # PV1.12 - Preadmit Test Indicator
        pv1_13 = pv1_13 # PV1.13 - Re-admission Indicator
        pv1_14 = pv1_14 # PV1.14 - Admit Source
        pv1_15 = pv1_15 # PV1.15 - Ambulatory Status
        pv1_16 = pv1_16 # PV1.16 - VIP Indicator
        pv1_17 = pv1_17 # PV1.17 - Admitting Doctor
        pv1_18 = pv1_18 # PV1.18 - Patient Type
        pv1_19 = pv1_19 # PV1.19 - Visit Number
        pv1_20 = pv1_20 # PV1.20 - Financial Class
        pv1_21 = pv1_21 # PV1.21 - Charge Price Indicator
        pv1_22 = pv1_22 # PV1.22 - Courtesy Code
        pv1_23 = pv1_23 # PV1.23 - Credit Rating
        pv1_24 = pv1_24 # PV1.24 - Contract Code
        pv1_25 = pv1_25 # PV1.25 - Contract Effective Date
        pv1_26 = pv1_26 # PV1.26 - Contract Amount
        pv1_27 = pv1_27 # PV1.27 - Contract Period
        pv1_28 = pv1_28 # PV1.28 - Interest Code
        pv1_29 = pv1_29 # PV1.29 - Transfer to Bad Debt Code
        pv1_30 = pv1_30 # PV1.30 - Transfer to Bad Debt Date
        pv1_31 = pv1_31 # PV1.31 - Bad Debt Agency Code
        pv1_32 = pv1_32 # PV1.32 - Bad Debt Transfer Amount
        pv1_33 = pv1_33 # PV1.33 - Bad Debt Recovery Amount
        pv1_34 = pv1_34 # PV1.34 - Delete Account Indicator
        pv1_35 = pv1_35 # PV1.35 - Delete Account Date
        pv1_36 = pv1_36 # PV1.36 - Discharge Disposition
        pv1_37 = pv1_37 # PV1.37 - Discharged to Location
        pv1_38 = pv1_38 # PV1.38 - Diet Type
        pv1_39 = pv1_39 # PV1.39 - Servicing Facility
        pv1_40 = pv1_40 # PV1.40 - Bed Status
        pv1_41 = pv1_41 # PV1.41 - Account Status
        pv1_42 = pv1_42 # PV1.42 - Pending Location
        pv1_43 = pv1_43 # PV1.43 - Prior Temporary Location
        pv1_44 = pv1_44 # PV1.44 - Admit Date/Time
        pv1_45 = pv1_45 # PV1.45 - Discharge Date/Time
        pv1_46 = pv1_46 # PV1.46 - Current Patient Balance
        pv1_47 = pv1_47 # PV1.47 - Total Charges
        pv1_48 = pv1_48 # PV1.48 - Total Adjustments
        pv1_49 = pv1_49 # PV1.49 - Total Payments
        pv1_50 = pv1_50 # PV1.50 - Alternate Visit ID
        pv1_51 = pv1_51 # PV1.51 - Visit Indicator
        pv1_52 = pv1_52 # PV1.52 - Other Healthcare Provider
        PV1 = ()
        return PV1   
    #This segment is a continuation of information contained on the PV1 segment.
    def PV2(self):
        # Optional for V1.0
        pass

    #This segment is used to transmit fields that are common to all orders (all types of services that are requested).
    def ORC(self):
        # Optional for V1.0
        pass        
    #This segment is used to transmit information specific to an order for a diagnostic study or observation, physical exam, or assessment.
    def OBR():
        obr_1 = obr_1  # OBR.1 - Set ID - OBR
        obr_2 = obr_2  # OBR.2 - Placer Order Number
        obr_2_1 = obr_2_1  # OBR.2.1 - Entity Identifier
        obr_2_2 = obr_2_2  # OBR.2.2 - Namespace Id
        obr_2_3 = obr_2_3  # OBR.2.3 - Universal Id
        obr_2_4 = obr_2_4  # OBR.2.4 - Universal Id Type
        obr_3 = obr_3  # OBR.3 - Filler Order Number
        obr_3_1 = obr_3_1  # OBR.3.1 - Entity Identifier
        obr_3_2 = obr_3_2  # OBR.3.2 - Namespace Id
        obr_3_3 = obr_3_3  # OBR.3.3 - Universal Id
        obr_3_4 = obr_3_4  # OBR.3.4 - Universal Id Type
        obr_4 = obr_4  # OBR.4 - Universal Service Identifier
        obr_4_1 = obr_4_1  # OBR.4.1 - Identifier
        obr_4_2 = obr_4_2  # OBR.4.2 - Text
        obr_4_3 = obr_4_3  # OBR.4.3 - Name Of Coding System
        obr_4_4 = obr_4_4  # OBR.4.4 - Alternate Identifier
        obr_4_5 = obr_4_5  # OBR.4.5 - Alternate Text
        obr_4_6 = obr_4_6  # OBR.4.6 - Name Of Alternate Coding System
        obr_5 = obr_5  # OBR.5 - Priority
        obr_6 = obr_6  # OBR.6 - Requested Date/Time
        obr_6_1 = obr_6_1  # OBR.6.1 - Time
        obr_6_2 = obr_6_2  # OBR.6.2 - Degree Of Precision
        obr_7 = obr_7  # OBR.7 - Observation Date/Time
        obr_7_1 = obr_7_1  # OBR.7.1 - Time
        obr_7_2 = obr_7_2  # OBR.7.2 - Degree Of Precision
        obr_8 = obr_8  # OBR.8 - Observation End Date/Time
        obr_8_1 = obr_8_1  # OBR.8.1 - Time
        obr_8_2 = obr_8_2  # OBR.8.2 - Degree Of Precision
        obr_9 = obr_9  # OBR.9 - Collection Volume
        obr_9_1 = obr_9_1  # OBR.9.1 - Quantity
        obr_9_2 = obr_9_2  # OBR.9.2 - Units
        obr_10 = obr_10  # OBR.10 - Collector Identifier
        obr_10_1 = obr_10_1  # OBR.10.1 - Id Number
        obr_10_2 = obr_10_2  # OBR.10.2 - Family Name
        obr_10_2_1 = obr_10_2_1  # OBR.10.2.1 - Surname
        obr_10_2_2 = obr_10_2_2  # OBR.10.2.2 - Own Surname Prefix
        obr_10_2_3 = obr_10_2_3  # OBR.10.2.3 - Own Surname
        obr_10_2_4 = obr_10_2_4  # OBR.10.2.4 - Surname Prefix From Partner/Spouse
        obr_10_2_5 = obr_10_2_5  # OBR.10.2.5 - Surname From Partner/Spouse
        obr_10_3 = obr_10_3  # OBR.10.3 - Given Name
        obr_10_4 = obr_10_4  # OBR.10.4 - Second And Further Given Names Or Initials Thereof
        obr_10_5 = obr_10_5  # OBR.10.5 - Suffix (e.g., Jr Or Iii)
        obr_10_6 = obr_10_6  # OBR.10.6 - Prefix (e.g., Dr)
        obr_10_7 = obr_10_7  # OBR.10.7 - Degree (e.g., Md)
        obr_10_8 = obr_10_8  # OBR.10.8 - Source Table
        obr_10_9 = obr_10_9  # OBR.10.9 - Assigning Authority
        obr_10_9_1 = obr_10_9_1  # OBR.10.9.1 - Namespace Id
        obr_10_9_2 = obr_10_9_2  # OBR.10.9.2 - Universal Id
        obr_10_9_3 = obr_10_9_3  # OBR.10.9.3 - Universal Id Type
        obr_10_10 = obr_10_10  # OBR.10.10 - Name Type Code
        obr_10_11 = obr_10_11  # OBR.10.11 - Identifier Check Digit
        obr_10_12 = obr_10_12  # OBR.10.12 - Check Digit Scheme
        obr_10_13 = obr_10_13  # OBR.10.13 - Identifier Type Code
        obr_10_14 = obr_10_14  # OBR.10.14 - Assigning Facility
        obr_10_14_1 = obr_10_14_1  # OBR.10.14.1 - Namespace Id
        obr_10_14_2 = obr_10_14_2  # OBR.10.14.2 - Universal Id
        obr_10_14_3 = obr_10_14_3  # OBR.10.14.3 - Universal Id Type
        obr_10_15 = obr_10_15  # OBR.10.15 - Name Representation Code
        obr_10_16 = obr_10_16  # OBR.10.16 - Name Context
        obr_10_16_1 = obr_10_16_1  # OBR.10.16.1 - Identifier
        obr_10_16_2 = obr_10_16_2  # OBR.10.16.2 - Text
        obr_10_16_3 = obr_10_16_3  # OBR.10.16.3 - Name Of Coding System
        obr_10_16_4 = obr_10_16_4  # OBR.10.16.4 - Alternate Identifier
        obr_10_16_5 = obr_10_16_5  # OBR.10.16.5 - Alternate Text
        obr_10_16_6 = obr_10_16_6  # OBR.10.16.6 - Name Of Alternate Coding System
        obr_10_17 = obr_10_17  # OBR.10.17 - Name Validity Range
        obr_10_17_1 = obr_10_17_1  # OBR.10.17.1 - Range Start Date/Time
        obr_10_17_1_1 = obr_10_17_1_1  # OBR.10.17.1.1 - Time
        obr_10_17_1_2 = obr_10_17_1_2  # OBR.10.17.1.2 - Degree Of Precision
        obr_10_17_2 = obr_10_17_2  # OBR.10.17.2 - Range End Date/Time
        obr_10_17_2_1 = obr_10_17_2_1  # OBR.10.17.2.1 - Time
        obr_10_17_2_2 = obr_10_17_2_2  # OBR.10.17.2.2 - Degree Of Precision
        obr_10_18 = obr_10_18  # OBR.10.18 - Name Assembly Order
        obr_10_19 = obr_10_19  # OBR.10.19 - Effective Date
        obr_10_19_1 = obr_10_19_1  # OBR.10.19.1 - Time
        obr_10_19_2 = obr_10_19_2  # OBR.10.19.2 - Degree Of Precision
        obr_10_20 = obr_10_20  # OBR.10.20 - Expiration Date
        obr_10_20_1 = obr_10_20_1  # OBR.10.20.1 - Time
        obr_10_20_2 = obr_10_20_2  # OBR.10.20.2 - Degree Of Precision
        obr_10_21 = obr_10_21  # OBR.10.21 - Professional Suffix
        obr_10_22 = obr_10_22  # OBR.10.22 - Assigning Jurisdiction
        obr_10_22_1 = obr_10_22_1  # OBR.10.22.1 - Identifier
        obr_10_22_2 = obr_10_22_2  # OBR.10.22.2 - Text
        obr_10_22_3 = obr_10_22_3  # OBR.10.22.3 - Name Of Coding System
        obr_10_22_4 = obr_10_22_4  # OBR.10.22.4 - Alternate Identifier
        obr_10_22_5 = obr_10_22_5  # OBR.10.22.5 - Alternate Text
        obr_10_22_6 = obr_10_22_6  # OBR.10.22.6 - Name Of Alternate Coding System
        obr_10_22_7 = obr_10_22_7  # OBR.10.22.7 - Coding System Version Id
        obr_10_22_8 = obr_10_22_8  # OBR.10.22.8 - Alternate Coding System Version Id
        obr_10_22_9 = obr_10_22_9  # OBR.10.22.9 - Original Text
        obr_10_23 = obr_10_23  # OBR.10.23 - Assigning Agency Or Department
        obr_10_23_1 = obr_10_23_1  # OBR.10.23.1 - Identifier
        obr_10_23_2 = obr_10_23_2  # OBR.10.23.2 - Text
        obr_10_23_3 = obr_10_23_3  # OBR.10.23.3 - Name Of Coding System
        obr_10_23_4 = obr_10_23_4  # OBR.10.23.4 - Alternate Identifier
        obr_10_23_5 = obr_10_23_5  # OBR.10.23.5 - Alternate Text
        obr_10_23_6 = obr_10_23_6  # OBR.10.23.6 - Name Of Alternate Coding System
        obr_10_23_7 = obr_10_23_7  # OBR.10.23.7 - Coding System Version Id
        obr_10_23_8 = obr_10_23_8  # OBR.10.23.8 - Alternate Coding System Version Id
        obr_10_23_9 = obr_10_23_9  # OBR.10.23.9 - Original Text
        obr_11 = obr_11  # OBR.11 - Specimen Action Code
        obr_12 = obr_12  # OBR.12 - Danger Code
        obr_12_1 = obr_12_1  # OBR.12.1 - Identifier
        obr_12_2 = obr_12_2  # OBR.12.2 - Text
        obr_12_3 = obr_12_3  # OBR.12.3 - Name Of Coding System
        obr_12_4 = obr_12_4  # OBR.12.4 - Alternate Identifier
        obr_12_5 = obr_12_5  # OBR.12.5 - Alternate Text
        obr_12_6 = obr_12_6  # OBR.12.6 - Name Of Alternate Coding System
        obr_13 = obr_13  # OBR.13 - Relevant Clinical Information
        obr_14 = obr_14  # OBR.14 - Specimen Received Date/Time
        obr_14_1 = obr_14_1  # OBR.14.1 - Time
        obr_14_2 = obr_14_2  # OBR.14.2 - Degree Of Precision
        obr_15 = obr_15  # OBR.15 - Specimen Source
        obr_15_1 = obr_15_1  # OBR.15.1 - Specimen Source Name Or Code
        obr_15_1_1 = obr_15_1_1  # OBR.15.1.1 - Identifier
        obr_15_1_2 = obr_15_1_2  # OBR.15.1.2 - Text
        obr_15_1_3 = obr_15_1_3  # OBR.15.1.3 - Name Of Coding System
        obr_15_1_4 = obr_15_1_4  # OBR.15.1.4 - Alternate Identifier
        obr_15_1_5 = obr_15_1_5  # OBR.15.1.5 - Alternate Text
        obr_15_1_6 = obr_15_1_6  # OBR.15.1.6 - Name Of Alternate Coding System
        obr_15_1_7 = obr_15_1_7  # OBR.15.1.7 - Coding System Version Id
        obr_15_1_8 = obr_15_1_8  # OBR.15.1.8 - Alternate Coding System Version Id
        obr_15_1_9 = obr_15_1_9  # OBR.15.1.9 - Original Text
        obr_15_2 = obr_15_2  # OBR.15.2 - Additives
        obr_15_2_1 = obr_15_2_1  # OBR.15.2.1 - Identifier
        obr_15_2_2 = obr_15_2_2  # OBR.15.2.2 - Text
        obr_15_2_3 = obr_15_2_3  # OBR.15.2.3 - Name Of Coding System
        obr_15_2_4 = obr_15_2_4  # OBR.15.2.4 - Alternate Identifier
        obr_15_2_5 = obr_15_2_5  # OBR.15.2.5 - Alternate Text
        obr_15_2_6 = obr_15_2_6  # OBR.15.2.6 - Name Of Alternate Coding System
        obr_15_2_7 = obr_15_2_7  # OBR.15.2.7 - Coding System Version Id
        obr_15_2_8 = obr_15_2_8  # OBR.15.2.8 - Alternate Coding System Version Id
        obr_15_2_9 = obr_15_2_9  # OBR.15.2.9 - Original Text
        obr_15_3 = obr_15_3  # OBR.15.3 - Specimen Collection Method
        obr_15_4 = obr_15_4  # OBR.15.4 - Body Site
        obr_15_4_1 = obr_15_4_1  # OBR.15.4.1 - Identifier
        obr_15_4_2 = obr_15_4_2  # OBR.15.4.2 - Text
        obr_15_4_3 = obr_15_4_3  # OBR.15.4.3 - Name Of Coding System
        obr_15_4_4 = obr_15_4_4  # OBR.15.4.4 - Alternate Identifier
        obr_15_4_5 = obr_15_4_5  # OBR.15.4.5 - Alternate Text
        obr_15_4_6 = obr_15_4_6  # OBR.15.4.6 - Name Of Alternate Coding System
        obr_15_4_7 = obr_15_4_7  # OBR.15.4.7 - Coding System Version Id
        obr_15_4_8 = obr_15_4_8  # OBR.15.4.8 - Alternate Coding System Version Id
        obr_15_4_9 = obr_15_4_9  # OBR.15.4.9 - Original Text
        obr_15_5 = obr_15_5  # OBR.15.5 - Site Modifier
        obr_15_5_1 = obr_15_5_1  # OBR.15.5.1 - Identifier
        obr_15_5_2 = obr_15_5_2  # OBR.15.5.2 - Text
        obr_15_5_3 = obr_15_5_3  # OBR.15.5.3 - Name Of Coding System
        obr_15_5_4 = obr_15_5_4  # OBR.15.5.4 - Alternate Identifier
        obr_15_5_5 = obr_15_5_5  # OBR.15.5.5 - Alternate Text
        obr_15_5_6 = obr_15_5_6  # OBR.15.5.6 - Name Of Alternate Coding System
        obr_15_5_7 = obr_15_5_7  # OBR.15.5.7 - Coding System Version Id
        obr_15_5_8 = obr_15_5_8  # OBR.15.5.8 - Alternate Coding System Version Id
        obr_15_5_9 = obr_15_5_9  # OBR.15.5.9 - Original Text
        obr_15_6 = obr_15_6  # OBR.15.6 - Collection Method Modifier Code
        obr_15_6_1 = obr_15_6_1  # OBR.15.6.1 - Identifier
        obr_15_6_2 = obr_15_6_2  # OBR.15.6.2 - Text
        obr_15_6_3 = obr_15_6_3  # OBR.15.6.3 - Name Of Coding System
        obr_15_6_4 = obr_15_6_4  # OBR.15.6.4 - Alternate Identifier
        obr_15_6_5 = obr_15_6_5  # OBR.15.6.5 - Alternate Text
        obr_15_6_6 = obr_15_6_6  # OBR.15.6.6 - Name Of Alternate Coding System
        obr_15_6_7 = obr_15_6_7  # OBR.15.6.7 - Coding System Version Id
        obr_15_6_8 = obr_15_6_8  # OBR.15.6.8 - Alternate Coding System Version Id
        obr_15_6_9 = obr_15_6_9  # OBR.15.6.9 - Original Text
        obr_15_7 = obr_15_7  # OBR.15.7 - Specimen Role
        obr_15_7_1 = obr_15_7_1  # OBR.15.7.1 - Identifier
        obr_15_7_2 = obr_15_7_2  # OBR.15.7.2 - Text
        obr_15_7_3 = obr_15_7_3  # OBR.15.7.3 - Name Of Coding System
        obr_15_7_4 = obr_15_7_4  # OBR.15.7.4 - Alternate Identifier
        obr_15_7_5 = obr_15_7_5  # OBR.15.7.5 - Alternate Text
        obr_15_7_6 = obr_15_7_6  # OBR.15.7.6 - Name Of Alternate Coding System
        obr_15_7_7 = obr_15_7_7  # OBR.15.7.7 - Coding System Version Id
        obr_15_7_8 = obr_15_7_8  # OBR.15.7.8 - Alternate Coding System Version Id
        obr_15_7_9 = obr_15_7_9  # OBR.15.7.9 - Original Text
        obr_16 = obr_16  # OBR.16 - Ordering Provider
        obr_16_1 = obr_16_1  # OBR.16.1 - Id Number
        obr_16_2 = obr_16_2  # OBR.16.2 - Family Name
        obr_16_2_1 = obr_16_2_1  # OBR.16.2.1 - Surname
        obr_16_2_2 = obr_16_2_2  # OBR.16.2.2 - Own Surname Prefix
        obr_16_2_3 = obr_16_2_3  # OBR.16.2.3 - Own Surname
        obr_16_2_4 = obr_16_2_4  # OBR.16.2.4 - Surname Prefix From Partner/Spouse
        obr_16_2_5 = obr_16_2_5  # OBR.16.2.5 - Surname From Partner/Spouse
        obr_16_3 = obr_16_3  # OBR.16.3 - Given Name
        obr_16_4 = obr_16_4  # OBR.16.4 - Second And Further Given Names Or Initials Thereof
        obr_16_5 = obr_16_5  # OBR.16.5 - Suffix (e.g., Jr Or Iii)
        obr_16_6 = obr_16_6  # OBR.16.6 - Prefix (e.g., Dr)
        obr_16_7 = obr_16_7  # OBR.16.7 - Degree (e.g., Md)
        obr_16_8 = obr_16_8  # OBR.16.8 - Source Table
        obr_16_9 = obr_16_9  # OBR.16.9 - Assigning Authority
        obr_16_9_1 = obr_16_9_1  # OBR.16.9.1 - Namespace Id
        obr_16_9_2 = obr_16_9_2  # OBR.16.9.2 - Universal Id
        obr_16_9_3 = obr_16_9_3  # OBR.16.9.3 - Universal Id Type
        obr_16_10 = obr_16_10  # OBR.16.10 - Name Type Code
        obr_16_11 = obr_16_11  # OBR.16.11 - Identifier Check Digit
        obr_16_12 = obr_16_12  # OBR.16.12 - Check Digit Scheme
        obr_16_13 = obr_16_13  # OBR.16.13 - Identifier Type Code
        obr_16_14 = obr_16_14  # OBR.16.14 - Assigning Facility
        obr_16_14_1 = obr_16_14_1  # OBR.16.14.1 - Namespace Id
        obr_16_14_2 = obr_16_14_2  # OBR.16.14.2 - Universal Id
        obr_16_14_3 = obr_16_14_3  # OBR.16.14.3 - Universal Id Type
        obr_16_15 = obr_16_15  # OBR.16.15 - Name Representation Code
        obr_16_16 = obr_16_16  # OBR.16.16 - Name Context
        obr_16_16_1 = obr_16_16_1  # OBR.16.16.1 - Identifier
        obr_16_16_2 = obr_16_16_2  # OBR.16.16.2 - Text
        obr_16_16_3 = obr_16_16_3  # OBR.16.16.3 - Name Of Coding System
        obr_16_16_4 = obr_16_16_4  # OBR.16.16.4 - Alternate Identifier
        obr_16_16_5 = obr_16_16_5  # OBR.16.16.5 - Alternate Text
        obr_16_16_6 = obr_16_16_6  # OBR.16.16.6 - Name Of Alternate Coding System
        obr_16_17 = obr_16_17  # OBR.16.17 - Name Validity Range
        obr_16_17_1 = obr_16_17_1  # OBR.16.17.1 - Range Start Date/Time
        obr_16_17_1_1 = obr_16_17_1_1  # OBR.16.17.1.1 - Time
        obr_16_17_1_2 = obr_16_17_1_2  # OBR.16.17.1.2 - Degree Of Precision
        obr_16_17_2 = obr_16_17_2  # OBR.16.17.2 - Range End Date/Time
        obr_16_17_2_1 = obr_16_17_2_1  # OBR.16.17.2.1 - Time
        obr_16_17_2_2 = obr_16_17_2_2  # OBR.16.17.2.2 - Degree Of Precision
        obr_16_18 = obr_16_18  # OBR.16.18 - Name Assembly Order
        obr_16_19 = obr_16_19  # OBR.16.19 - Effective Date
        obr_16_19_1 = obr_16_19_1  # OBR.16.19.1 - Time
        obr_16_19_1 = obr_16_19_1  # OBR.16.19.1 - Degree Of Precision
        obr_16_20 = obr_16_20  # OBR.16.20 - Expiration Date
        obr_16_20_1 = obr_16_20_1  # OBR.16.20.1 - Time
        obr_16_20_1 = obr_16_20_1  # OBR.16.20.1 - Degree Of Precision
        obr_16_21 = obr_16_21  # OBR.16.21 - Professional Suffix
        obr_16_22 = obr_16_22  # OBR.16.22 - Assigning Jurisdiction
        obr_16_22_1 = obr_16_22_1  # OBR.16.22.1 - Identifier
        obr_16_22_2 = obr_16_22_2  # OBR.16.22.2 - Text
        obr_16_22_3 = obr_16_22_3  # OBR.16.22.3 - Name Of Coding System
        obr_16_22_4 = obr_16_22_4  # OBR.16.22.4 - Alternate Identifier
        obr_16_22_5 = obr_16_22_5  # OBR.16.22.5 - Alternate Text
        obr_16_22_6 = obr_16_22_6  # OBR.16.22.6 - Name Of Alternate Coding System
        obr_16_22_7 = obr_16_22_7  # OBR.16.22.7 - Coding System Version Id
        obr_16_22_8 = obr_16_22_8  # OBR.16.22.8 - Alternate Coding System Version Id
        obr_16_22_9 = obr_16_22_9  # OBR.16.22.9 - Original Text
        obr_16_23 = obr_16_23  # OBR.16.23 - Assigning Agency Or Department
        obr_16_23_1 = obr_16_23_1  # OBR.16.23.1 - Identifier
        obr_16_23_2 = obr_16_23_2  # OBR.16.23.2 - Text
        obr_16_23_3 = obr_16_23_3  # OBR.16.23.3 - Name Of Coding System
        obr_16_23_4 = obr_16_23_4  # OBR.16.23.4 - Alternate Identifier
        obr_16_23_5 = obr_16_23_5  # OBR.16.23.5 - Alternate Text
        obr_16_23_6 = obr_16_23_6  # OBR.16.23.6 - Name Of Alternate Coding System
        obr_16_23_7 = obr_16_23_7  # OBR.16.23.7 - Coding System Version Id
        obr_16_23_8 = obr_16_23_8  # OBR.16.23.8 - Alternate Coding System Version Id
        obr_16_23_9 = obr_16_23_9  # OBR.16.23.9 - Original Text
        obr_17 = obr_17  # OBR.17 - Order Callback Phone Number
        obr_17_1 = obr_17_1  # OBR.17.1 - Telephone Number
        obr_17_2 = obr_17_2  # OBR.17.2 - Telecommunication Use Code
        obr_17_3 = obr_17_3  # OBR.17.3 - Telecommunication Equipment Type
        obr_17_4 = obr_17_4  # OBR.17.4 - Email Address
        obr_17_5 = obr_17_5  # OBR.17.5 - Country Code
        obr_17_6 = obr_17_6  # OBR.17.6 - Area/City Code
        obr_17_7 = obr_17_7  # OBR.17.7 - Local Number
        obr_17_8 = obr_17_8  # OBR.17.8 - Extension
        obr_17_9 = obr_17_9  # OBR.17.9 - Any Text
        obr_17_10 = obr_17_10  # OBR.17.10 - Extension Prefix
        obr_17_11 = obr_17_11  # OBR.17.11 - Speed Dial Code
        obr_17_12 = obr_17_12  # OBR.17.12 - Unformatted Telephone Number
        obr_18 = obr_18  # OBR.18 - Placer Field 1
        obr_19 = obr_19  # OBR.19 - Placer Field 2
        obr_20 = obr_20  # OBR.20 - Filler Field 1
        obr_21 = obr_21  # OBR.21 - Filler Field 2
        obr_22 = obr_22  # OBR.22 - Results Rpt/Status Chng
        obr_22_1 = obr_22_1  # OBR.22.1 - Time
        obr_22_1 = obr_22_1  # OBR.22.1 - Degree Of Precision
        obr_23 = obr_23  # OBR.23 - Charge to Practice
        obr_23_1 = obr_23_1  # OBR.23.1 - Monetary Amount
        obr_23_1_1 = obr_23_1_1  # OBR.23.1.1 - Quantity
        obr_23_1_2 = obr_23_1_2  # OBR.23.1.2 - Denomination
        obr_23_2 = obr_23_2  # OBR.23.2 - Charge Code
        obr_23_2_1 = obr_23_2_1  # OBR.23.2.1 - Identifier
        obr_23_2_2 = obr_23_2_2  # OBR.23.2.2 - Text
        obr_23_2_3 = obr_23_2_3  # OBR.23.2.3 - Name Of Coding System
        obr_23_2_4 = obr_23_2_4  # OBR.23.2.4 - Alternate Identifier
        obr_23_2_5 = obr_23_2_5  # OBR.23.2.5 - Alternate Text
        obr_23_2_6 = obr_23_2_6  # OBR.23.2.6 - Name Of Alternate Coding System
        obr_24 = obr_24  # OBR.24 - Diagnostic Serv Sect ID
        obr_25 = obr_25  # OBR.25 - Result Status
        obr_26 = obr_26  # OBR.26 - Parent Result
        obr_26_1 = obr_26_1  # OBR.26.1 - Parent Observation Identifier
        obr_26_1_1 = obr_26_1_1  # OBR.26.1.1 - Identifier
        obr_26_1_2 = obr_26_1_2  # OBR.26.1.2 - Text
        obr_26_1_3 = obr_26_1_3  # OBR.26.1.3 - Name Of Coding System
        obr_26_1_4 = obr_26_1_4  # OBR.26.1.4 - Alternate Identifier
        obr_26_1_5 = obr_26_1_5  # OBR.26.1.5 - Alternate Text
        obr_26_1_6 = obr_26_1_6  # OBR.26.1.6 - Name Of Alternate Coding System
        obr_26_2 = obr_26_2  # OBR.26.2 - Parent Observation Sub
        obr_26_3 = obr_26_3  # OBR.26.3 - Parent Observation Value Descriptor
        obr_27 = obr_27  # OBR.27 - Quantity/Timing
        obr_27_1 = obr_27_1  # OBR.27.1 - Quantity
        obr_27_1_1 = obr_27_1_1  # OBR.27.1.1 - Quantity
        obr_27_1_2 = obr_27_1_2  # OBR.27.1.2 - Units
        obr_27_1_1 = obr_27_1_1  # OBR.27.1.1 - Identifier
        obr_27_1_2 = obr_27_1_2  # OBR.27.1.2 - Text
        obr_27_1_3 = obr_27_1_3  # OBR.27.1.3 - Name Of Coding System
        obr_27_1_4 = obr_27_1_4  # OBR.27.1.4 - Alternate Identifier
        obr_27_1_5 = obr_27_1_5  # OBR.27.1.5 - Alternate Text
        obr_27_1_6 = obr_27_1_6  # OBR.27.1.6 - Name Of Alternate Coding System
        obr_27_2 = obr_27_2  # OBR.27.2 - Interval
        obr_27_2_1 = obr_27_2_1  # OBR.27.2.1 - Repeat Pattern
        obr_27_2_2 = obr_27_2_2  # OBR.27.2.2 - Explicit Time Interval
        obr_27_3 = obr_27_3  # OBR.27.3 - Duration
        obr_27_4 = obr_27_4  # OBR.27.4 - Start Date/Time
        obr_27_4_1 = obr_27_4_1  # OBR.27.4.1 - Time
        obr_27_4_2 = obr_27_4_2  # OBR.27.4.2 - Degree Of Precision
        obr_27_5 = obr_27_5  # OBR.27.5 - End Date/Time
        obr_27_5_1 = obr_27_5_1  # OBR.27.5.1 - Time
        obr_27_5_2 = obr_27_5_2  # OBR.27.5.2 - Degree Of Precision
        obr_27_6 = obr_27_6  # OBR.27.6 - Priority
        obr_27_7 = obr_27_7  # OBR.27.7 - Condition
        obr_27_8 = obr_27_8  # OBR.27.8 - Text
        obr_27_9 = obr_27_9  # OBR.27.9 - Conjunction
        obr_27_10 = obr_27_10  # OBR.27.10 - Order Sequencing
        obr_27_10_1 = obr_27_10_1  # OBR.27.10.1 - Sequence/Results Flag
        obr_27_10_2 = obr_27_10_2  # OBR.27.10.2 - Placer Order Number: Entity Identifier
        obr_27_10_3 = obr_27_10_3  # OBR.27.10.3 - Placer Order Number: Namespace Id
        obr_27_10_4 = obr_27_10_4  # OBR.27.10.4 - Filler Order Number: Entity Identifier
        obr_27_10_5 = obr_27_10_5  # OBR.27.10.5 - Filler Order Number: Namespace Id
        obr_27_10_6 = obr_27_10_6  # OBR.27.10.6 - Sequence Condition Value
        obr_27_10_7 = obr_27_10_7  # OBR.27.10.7 - Maximum Number Of Repeats
        obr_27_10_8 = obr_27_10_8  # OBR.27.10.8 - Placer Order Number: Universal Id
        obr_27_10_9 = obr_27_10_9  # OBR.27.10.9 - Placer Order Number: Universal Id Type
        obr_27_10_10 = obr_27_10_10  # OBR.27.10.10 - Filler Order Number: Universal Id
        obr_27_10_11 = obr_27_10_11  # OBR.27.10.11 - Filler Order Number: Universal Id Type
        obr_27_11 = obr_27_11  # OBR.27.11 - Occurrence Duration
        obr_27_11_1 = obr_27_11_1  # OBR.27.11.1 - Identifier
        obr_27_11_2 = obr_27_11_2  # OBR.27.11.2 - Text
        obr_27_11_3 = obr_27_11_3  # OBR.27.11.3 - Name Of Coding System
        obr_27_11_4 = obr_27_11_4  # OBR.27.11.4 - Alternate Identifier
        obr_27_11_5 = obr_27_11_5  # OBR.27.11.5 - Alternate Text
        obr_27_11_6 = obr_27_11_6  # OBR.27.11.6 - Name Of Alternate Coding System
        obr_27_12 = obr_27_12  # OBR.27.12 - Total Occurrences
        obr_28 = obr_28  # OBR.28 - Result Copies To
        obr_28_1 = obr_28_1  # OBR.28.1 - Id Number
        obr_28_2 = obr_28_2  # OBR.28.2 - Family Name
        obr_28_2_1 = obr_28_2_1  # OBR.28.2.1 - Surname
        obr_28_2_2 = obr_28_2_2  # OBR.28.2.2 - Own Surname Prefix
        obr_28_2_3 = obr_28_2_3  # OBR.28.2.3 - Own Surname
        obr_28_2_4 = obr_28_2_4  # OBR.28.2.4 - Surname Prefix From Partner/Spouse
        obr_28_2_5 = obr_28_2_5  # OBR.28.2.5 - Surname From Partner/Spouse
        obr_28_3 = obr_28_3  # OBR.28.3 - Given Name
        obr_28_4 = obr_28_4  # OBR.28.4 - Second And Further Given Names Or Initials Thereof
        obr_28_5 = obr_28_5  # OBR.28.5 - Suffix (e.g., Jr Or Iii)
        obr_28_6 = obr_28_6  # OBR.28.6 - Prefix (e.g., Dr)
        obr_28_7 = obr_28_7  # OBR.28.7 - Degree (e.g., Md)
        obr_28_8 = obr_28_8  # OBR.28.8 - Source Table
        obr_28_9 = obr_28_9  # OBR.28.9 - Assigning Authority
        obr_28_9_1 = obr_28_9_1  # OBR.28.9.1 - Namespace Id
        obr_28_9_2 = obr_28_9_2  # OBR.28.9.2 - Universal Id
        obr_28_9_3 = obr_28_9_3  # OBR.28.9.3 - Universal Id Type
        obr_28_10 = obr_28_10  # OBR.28.10 - Name Type Code
        obr_28_11 = obr_28_11  # OBR.28.11 - Identifier Check Digit
        obr_28_12 = obr_28_12  # OBR.28.12 - Check Digit Scheme
        obr_28_13 = obr_28_13  # OBR.28.13 - Identifier Type Code
        obr_28_14 = obr_28_14  # OBR.28.14 - Assigning Facility
        obr_28_14_1 = obr_28_14_1  # OBR.28.14.1 - Namespace Id
        obr_28_14_2 = obr_28_14_2  # OBR.28.14.2 - Universal Id
        obr_28_14_3 = obr_28_14_3  # OBR.28.14.3 - Universal Id Type
        obr_28_15 = obr_28_15  # OBR.28.15 - Name Representation Code
        obr_28_16 = obr_28_16  # OBR.28.16 - Name Context
        obr_28_16_1 = obr_28_16_1  # OBR.28.16.1 - Identifier
        obr_28_16_2 = obr_28_16_2  # OBR.28.16.2 - Text
        obr_28_16_3 = obr_28_16_3  # OBR.28.16.3 - Name Of Coding System
        obr_28_16_4 = obr_28_16_4  # OBR.28.16.4 - Alternate Identifier
        obr_28_16_5 = obr_28_16_5  # OBR.28.16.5 - Alternate Text
        obr_28_16_6 = obr_28_16_6  # OBR.28.16.6 - Name Of Alternate Coding System
        obr_28_17 = obr_28_17  # OBR.28.17 - Name Validity Range
        obr_28_17_1 = obr_28_17_1  # OBR.28.17.1 - Range Start Date/Time
        obr_28_17_1_1 = obr_28_17_1_1  # OBR.28.17.1.1 - Time
        obr_28_17_1_2 = obr_28_17_1_2  # OBR.28.17.1.2 - Degree Of Precision
        obr_28_17_2 = obr_28_17_2  # OBR.28.17.2 - Range End Date/Time
        obr_28_17_2_1 = obr_28_17_2_1  # OBR.28.17.2.1 - Time
        obr_28_17_2_2 = obr_28_17_2_2  # OBR.28.17.2.2 - Degree Of Precision
        obr_28_18 = obr_28_18  # OBR.28.18 - Name Assembly Order
        obr_28_19 = obr_28_19  # OBR.28.19 - Effective Date
        obr_28_19_1 = obr_28_19_1  # OBR.28.19.1 - Time
        obr_28_19_2 = obr_28_19_2  # OBR.28.19.2 - Degree Of Precision
        obr_28_20 = obr_28_20  # OBR.28.20 - Expiration Date
        obr_28_20_1 = obr_28_20_1  # OBR.28.20.1 - Time
        obr_28_20_2 = obr_28_20_2  # OBR.28.20.2 - Degree Of Precision
        obr_28_21 = obr_28_21  # OBR.28.21 - Professional Suffix
        obr_28_22 = obr_28_22  # OBR.28.22 - Assigning Jurisdiction
        obr_28_22_1 = obr_28_22_1  # OBR.28.22.1 - Identifier
        obr_28_22_2 = obr_28_22_2  # OBR.28.22.2 - Text
        obr_28_22_3 = obr_28_22_3  # OBR.28.22.3 - Name Of Coding System
        obr_28_22_4 = obr_28_22_4  # OBR.28.22.4 - Alternate Identifier
        obr_28_22_5 = obr_28_22_5  # OBR.28.22.5 - Alternate Text
        obr_28_22_6 = obr_28_22_6  # OBR.28.22.6 - Name Of Alternate Coding System
        obr_28_22_7 = obr_28_22_7  # OBR.28.22.7 - Coding System Version Id
        obr_28_22_8 = obr_28_22_8  # OBR.28.22.8 - Alternate Coding System Version Id
        obr_28_22_9 = obr_28_22_9  # OBR.28.22.9 - Original Text
        obr_28_23 = obr_28_23  # OBR.28.23 - Assigning Agency Or Department
        obr_28_23_1 = obr_28_23_1  # OBR.28.23.1 - Identifier
        obr_28_23_2 = obr_28_23_2  # OBR.28.23.2 - Text
        obr_28_23_3 = obr_28_23_3  # OBR.28.23.3 - Name Of Coding System
        obr_28_23_4 = obr_28_23_4  # OBR.28.23.4 - Alternate Identifier
        obr_28_23_5 = obr_28_23_5  # OBR.28.23.5 - Alternate Text
        obr_28_23_6 = obr_28_23_6  # OBR.28.23.6 - Name Of Alternate Coding System
        obr_28_23_7 = obr_28_23_7  # OBR.28.23.7 - Coding System Version Id
        obr_28_23_8 = obr_28_23_8  # OBR.28.23.8 - Alternate Coding System Version Id
        obr_28_23_9 = obr_28_23_9  # OBR.28.23.9 - Original Text
        obr_29 = obr_29  # OBR.29 - Parent
        obr_29_1 = obr_29_1  # OBR.29.1 - Placer Assigned Identifier
        obr_29_1_1 = obr_29_1_1  # OBR.29.1.1 - Entity Identifier
        obr_29_1_2 = obr_29_1_2  # OBR.29.1.2 - Namespace Id
        obr_29_1_3 = obr_29_1_3  # OBR.29.1.3 - Universal Id
        obr_29_1_4 = obr_29_1_4  # OBR.29.1.4 - Universal Id Type
        obr_29_2 = obr_29_2  # OBR.29.2 - Filler Assigned Identifier
        obr_29_2_1 = obr_29_2_1  # OBR.29.2.1 - Entity Identifier
        obr_29_2_2 = obr_29_2_2  # OBR.29.2.2 - Namespace Id
        obr_29_2_3 = obr_29_2_3  # OBR.29.2.3 - Universal Id
        obr_29_2_4 = obr_29_2_4  # OBR.29.2.4 - Universal Id Type
        obr_30 = obr_30  # OBR.30 - Transportation Mode
        obr_31 = obr_31  # OBR.31 - Reason for Study
        obr_31_1 = obr_31_1  # OBR.31.1 - Identifier
        obr_31_2 = obr_31_2  # OBR.31.2 - Text
        obr_31_3 = obr_31_3  # OBR.31.3 - Name Of Coding System
        obr_31_4 = obr_31_4  # OBR.31.4 - Alternate Identifier
        obr_31_5 = obr_31_5  # OBR.31.5 - Alternate Text
        obr_31_6 = obr_31_6  # OBR.31.6 - Name Of Alternate Coding System
        obr_32 = obr_32  # OBR.32 - Principal Result Interpreter
        obr_32_1 = obr_32_1  # OBR.32.1 - Name
        obr_32_1_1 = obr_32_1_1  # OBR.32.1.1 - Id Number
        obr_32_1_2 = obr_32_1_2  # OBR.32.1.2 - Family Name
        obr_32_1_3 = obr_32_1_3  # OBR.32.1.3 - Given Name
        obr_32_1_4 = obr_32_1_4  # OBR.32.1.4 - Second And Further Given Names Or Initials Thereof
        obr_32_1_5 = obr_32_1_5  # OBR.32.1.5 - Suffix
        obr_32_1_6 = obr_32_1_6  # OBR.32.1.6 - Prefix
        obr_32_1_7 = obr_32_1_7  # OBR.32.1.7 - Degree
        obr_32_1_8 = obr_32_1_8  # OBR.32.1.8 - Source Table
        obr_32_1_9 = obr_32_1_9  # OBR.32.1.9 - Assigning Authority
        obr_32_1_10 = obr_32_1_10  # OBR.32.1.10 - Assigning Authority
        obr_32_1_11 = obr_32_1_11  # OBR.32.1.11 - Assigning Authority
        obr_32_2 = obr_32_2  # OBR.32.2 - Start Date/Time
        obr_32_2_1 = obr_32_2_1  # OBR.32.2.1 - Time
        obr_32_2_2 = obr_32_2_2  # OBR.32.2.2 - Degree Of Precision
        obr_32_3 = obr_32_3  # OBR.32.3 - End Date/Time
        obr_32_3_1 = obr_32_3_1  # OBR.32.3.1 - Time
        obr_32_3_2 = obr_32_3_2  # OBR.32.3.2 - Degree Of Precision
        obr_32_4 = obr_32_4  # OBR.32.4 - Point Of Care
        obr_32_5 = obr_32_5  # OBR.32.5 - Room
        obr_32_6 = obr_32_6  # OBR.32.6 - Bed
        obr_32_7 = obr_32_7  # OBR.32.7 - Facility
        obr_32_7_1 = obr_32_7_1  # OBR.32.7.1 - Namespace Id
        obr_32_7_2 = obr_32_7_2  # OBR.32.7.2 - Universal Id
        obr_32_7_3 = obr_32_7_3  # OBR.32.7.3 - Universal Id Type
        obr_32_8 = obr_32_8  # OBR.32.8 - Location Status
        obr_32_9 = obr_32_9  # OBR.32.9 - Patient Location Type
        obr_32_10 = obr_32_10  # OBR.32.10 - Building
        obr_32_11 = obr_32_11  # OBR.32.11 - Floor
        obr_33 = obr_33  # OBR.33 - Assistant Result Interpreter
        obr_33_1 = obr_33_1  # OBR.33.1 - Name
        obr_33_1_1 = obr_33_1_1  # OBR.33.1.1 - Id Number
        obr_33_1_2 = obr_33_1_2  # OBR.33.1.2 - Family Name
        obr_33_1_3 = obr_33_1_3  # OBR.33.1.3 - Given Name
        obr_33_1_4 = obr_33_1_4  # OBR.33.1.4 - Second And Further Given Names Or Initials Thereof
        obr_33_1_5 = obr_33_1_5  # OBR.33.1.5 - Suffix
        obr_33_1_6 = obr_33_1_6  # OBR.33.1.6 - Prefix
        obr_33_1_7 = obr_33_1_7  # OBR.33.1.7 - Degree
        obr_33_1_8 = obr_33_1_8  # OBR.33.1.8 - Source Table
        obr_33_1_9 = obr_33_1_9  # OBR.33.1.9 - Assigning Authority
        obr_33_1_10 = obr_33_1_10  # OBR.33.1.10 - Assigning Authority
        obr_33_1_11 = obr_33_1_11  # OBR.33.1.11 - Assigning Authority
        obr_33_2 = obr_33_2  # OBR.33.2 - Start Date/Time
        obr_33_2_1 = obr_33_2_1  # OBR.33.2.1 - Time
        obr_33_2_2 = obr_33_2_2  # OBR.33.2.2 - Degree Of Precision
        obr_33_3 = obr_33_3  # OBR.33.3 - End Date/Time
        obr_33_3_1 = obr_33_3_1  # OBR.33.3.1 - Time
        obr_33_3_2 = obr_33_3_2  # OBR.33.3.2 - Degree Of Precision
        obr_33_4 = obr_33_4  # OBR.33.4 - Point Of Care
        obr_33_5 = obr_33_5  # OBR.33.5 - Room
        obr_33_6 = obr_33_6  # OBR.33.6 - Bed
        obr_33_7 = obr_33_7  # OBR.33.7 - Facility
        obr_33_7_1 = obr_33_7_1  # OBR.33.7.1 - Namespace Id
        obr_33_7_2 = obr_33_7_2  # OBR.33.7.2 - Universal Id
        obr_33_7_3 = obr_33_7_3  # OBR.33.7.3 - Universal Id Type
        obr_33_8 = obr_33_8  # OBR.33.8 - Location Status
        obr_33_9 = obr_33_9  # OBR.33.9 - Patient Location Type
        obr_33_10 = obr_33_10  # OBR.33.10 - Building
        obr_33_11 = obr_33_11  # OBR.33.11 - Floor
        obr_34 = obr_34  # OBR.34 - Technician
        obr_34_1 = obr_34_1  # OBR.34.1 - Name
        obr_34_1_1 = obr_34_1_1  # OBR.34.1.1 - Id Number
        obr_34_1_2 = obr_34_1_2  # OBR.34.1.2 - Family Name
        obr_34_1_3 = obr_34_1_3  # OBR.34.1.3 - Given Name
        obr_34_1_4 = obr_34_1_4  # OBR.34.1.4 - Second And Further Given Names Or Initials Thereof
        obr_34_1_5 = obr_34_1_5  # OBR.34.1.5 - Suffix
        obr_34_1_6 = obr_34_1_6  # OBR.34.1.6 - Prefix
        obr_34_1_7 = obr_34_1_7  # OBR.34.1.7 - Degree
        obr_34_1_8 = obr_34_1_8  # OBR.34.1.8 - Source Table
        obr_34_1_9 = obr_34_1_9  # OBR.34.1.9 - Assigning Authority
        obr_34_1_10 = obr_34_1_10  # OBR.34.1.10 - Assigning Authority
        obr_34_1_11 = obr_34_1_11  # OBR.34.1.11 - Assigning Authority
        obr_34_2 = obr_34_2  # OBR.34.2 - Start Date/Time
        obr_34_2_1 = obr_34_2_1  # OBR.34.2.1 - Time
        obr_34_2_2 = obr_34_2_2  # OBR.34.2.2 - Degree Of Precision
        obr_34_3 = obr_34_3  # OBR.34.3 - End Date/Time
        obr_34_3_1 = obr_34_3_1  # OBR.34.3.1 - Time
        obr_34_3_2 = obr_34_3_2  # OBR.34.3.2 - Degree Of Precision
        obr_34_4 = obr_34_4  # OBR.34.4 - Point Of Care
        obr_34_5 = obr_34_5  # OBR.34.5 - Room
        obr_34_6 = obr_34_6  # OBR.34.6 - Bed
        obr_34_7 = obr_34_7  # OBR.34.7 - Facility
        obr_34_7_1 = obr_34_7_1  # OBR.34.7.1 - Namespace Id
        obr_34_7_2 = obr_34_7_2  # OBR.34.7.2 - Universal Id
        obr_34_7_3 = obr_34_7_3  # OBR.34.7.3 - Universal Id Type
        obr_34_8 = obr_34_8  # OBR.34.8 - Location Status
        obr_34_9 = obr_34_9  # OBR.34.9 - Patient Location Type
        obr_34_10 = obr_34_10  # OBR.34.10 - Building
        obr_34_11 = obr_34_11  # OBR.34.11 - Floor
        obr_35 = obr_35  # OBR.35 - Transcriptionist
        obr_35_1 = obr_35_1  # OBR.35.1 - Name
        obr_35_1_1 = obr_35_1_1  # OBR.35.1.1 - Id Number
        obr_35_1_2 = obr_35_1_2  # OBR.35.1.2 - Family Name
        obr_35_1_3 = obr_35_1_3  # OBR.35.1.3 - Given Name
        obr_35_1_4 = obr_35_1_4  # OBR.35.1.4 - Second And Further Given Names Or Initials Thereof
        obr_35_1_5 = obr_35_1_5  # OBR.35.1.5 - Suffix
        obr_35_1_6 = obr_35_1_6  # OBR.35.1.6 - Prefix
        obr_35_1_7 = obr_35_1_7  # OBR.35.1.7 - Degree
        obr_35_1_8 = obr_35_1_8  # OBR.35.1.8 - Source Table
        obr_35_1_9 = obr_35_1_9  # OBR.35.1.9 - Assigning Authority
        obr_35_1_10 = obr_35_1_10  # OBR.35.1.10 - Assigning Authority
        obr_35_1_11 = obr_35_1_11  # OBR.35.1.11 - Assigning Authority
        obr_35_2 = obr_35_2  # OBR.35.2 - Start Date/Time
        obr_35_2_1 = obr_35_2_1  # OBR.35.2.1 - Time
        obr_35_2_2 = obr_35_2_2  # OBR.35.2.2 - Degree Of Precision
        obr_35_3 = obr_35_3  # OBR.35.3 - End Date/Time
        obr_35_3_1 = obr_35_3_1  # OBR.35.3.1 - Time
        obr_35_3_2 = obr_35_3_2  # OBR.35.3.2 - Degree Of Precision
        obr_35_4 = obr_35_4  # OBR.35.4 - Point Of Care
        obr_35_5 = obr_35_5  # OBR.35.5 - Room
        obr_35_6 = obr_35_6  # OBR.35.6 - Bed
        obr_35_7 = obr_35_7  # OBR.35.7 - Facility
        obr_35_7_1 = obr_35_7_1  # OBR.35.7.1 - Namespace Id
        obr_35_7_2 = obr_35_7_2  # OBR.35.7.2 - Universal Id
        obr_35_7_3 = obr_35_7_3  # OBR.35.7.3 - Universal Id Type
        obr_35_8 = obr_35_8  # OBR.35.8 - Location Status
        obr_35_9 = obr_35_9  # OBR.35.9 - Patient Location Type
        obr_35_10 = obr_35_10  # OBR.35.10 - Building
        obr_35_11 = obr_35_11  # OBR.35.11 - Floor
        obr_36 = obr_36  # OBR.36 - Scheduled Date/Time
        obr_36_1 = obr_36_1  # OBR.36.1 - Time
        obr_36_2 = obr_36_2  # OBR.36.2 - Degree Of Precision
        obr_37 = obr_37  # OBR.37 - Number of Sample Containers
        obr_38 = obr_38  # OBR.38 - Transport Logistics of Collected Sample
        obr_38_1 = obr_38_1  # OBR.38.1 - Identifier
        obr_38_2 = obr_38_2  # OBR.38.2 - Text
        obr_38_3 = obr_38_3  # OBR.38.3 - Name Of Coding System
        obr_38_4 = obr_38_4  # OBR.38.4 - Alternate Identifier
        obr_38_5 = obr_38_5  # OBR.38.5 - Alternate Text
        obr_38_6 = obr_38_6  # OBR.38.6 - Name Of Alternate Coding System
        obr_39 = obr_39  # OBR.39 - Collector's Comment
        obr_39_1 = obr_39_1  # OBR.39.1 - Identifier
        obr_39_2 = obr_39_2  # OBR.39.2 - Text
        obr_39_3 = obr_39_3  # OBR.39.3 - Name Of Coding System
        obr_39_4 = obr_39_4  # OBR.39.4 - Alternate Identifier
        obr_39_5 = obr_39_5  # OBR.39.5 - Alternate Text
        obr_39_6 = obr_39_6  # OBR.39.6 - Name Of Alternate Coding System
        obr_40 = obr_40  # OBR.40 - Transport Arrangement Responsibility
        obr_40_1 = obr_40_1  # OBR.40.1 - Identifier
        obr_40_2 = obr_40_2  # OBR.40.2 - Text
        obr_40_3 = obr_40_3  # OBR.40.3 - Name Of Coding System
        obr_40_4 = obr_40_4  # OBR.40.4 - Alternate Identifier
        obr_40_5 = obr_40_5  # OBR.40.5 - Alternate Text
        obr_40_6 = obr_40_6  # OBR.40.6 - Name Of Alternate Coding System
        obr_41 = obr_41  # OBR.41 - Transport Arranged
        obr_42 = obr_42  # OBR.42 - Escort Required
        obr_43 = obr_43  # OBR.43 - Planned Patient Transport Comment
        obr_43_1 = obr_43_1  # OBR.43.1 - Identifier
        obr_43_2 = obr_43_2  # OBR.43.2 - Text
        obr_43_3 = obr_43_3  # OBR.43.3 - Name Of Coding System
        obr_43_4 = obr_43_4  # OBR.43.4 - Alternate Identifier
        obr_43_5 = obr_43_5  # OBR.43.5 - Alternate Text
        obr_43_6 = obr_43_6  # OBR.43.6 - Name Of Alternate Coding System
        obr_44 = obr_44  # OBR.44 - Procedure Code
        obr_44_1 = obr_44_1  # OBR.44.1 - Identifier
        obr_44_2 = obr_44_2  # OBR.44.2 - Text
        obr_44_3 = obr_44_3  # OBR.44.3 - Name Of Coding System
        obr_44_4 = obr_44_4  # OBR.44.4 - Alternate Identifier
        obr_44_5 = obr_44_5  # OBR.44.5 - Alternate Text
        obr_44_6 = obr_44_6  # OBR.44.6 - Name Of Alternate Coding System
        obr_45 = obr_45  # OBR.45 - Procedure Code Modifier
        obr_45_1 = obr_45_1  # OBR.45.1 - Identifier
        obr_45_2 = obr_45_2  # OBR.45.2 - Text
        obr_45_3 = obr_45_3  # OBR.45.3 - Name Of Coding System
        obr_45_4 = obr_45_4  # OBR.45.4 - Alternate Identifier
        obr_45_5 = obr_45_5  # OBR.45.5 - Alternate Text
        obr_45_6 = obr_45_6  # OBR.45.6 - Name Of Alternate Coding System
        obr_46 = obr_46  # OBR.46 - Placer Supplemental Service Information
        obr_46_1 = obr_46_1  # OBR.46.1 - Identifier
        obr_46_2 = obr_46_2  # OBR.46.2 - Text
        obr_46_3 = obr_46_3  # OBR.46.3 - Name Of Coding System
        obr_46_4 = obr_46_4  # OBR.46.4 - Alternate Identifier
        obr_46_5 = obr_46_5  # OBR.46.5 - Alternate Text
        obr_46_6 = obr_46_6  # OBR.46.6 - Name Of Alternate Coding System
        obr_47 = obr_47  # OBR.47 - Filler Supplemental Service Information
        obr_47_1 = obr_47_1  # OBR.47.1 - Identifier
        obr_47_2 = obr_47_2  # OBR.47.2 - Text
        obr_47_3 = obr_47_3  # OBR.47.3 - Name Of Coding System
        obr_47_4 = obr_47_4  # OBR.47.4 - Alternate Identifier
        obr_47_5 = obr_47_5  # OBR.47.5 - Alternate Text
        obr_47_6 = obr_47_6  # OBR.47.6 - Name Of Alternate Coding System
        obr_48 = obr_48  # OBR.48 - Medically Necessary Duplicate Procedure Reason.
        obr_48_1 = obr_48_1  # OBR.48.1 - Identifier
        obr_48_2 = obr_48_2  # OBR.48.2 - Text
        obr_48_3 = obr_48_3  # OBR.48.3 - Name Of Coding System
        obr_48_4 = obr_48_4  # OBR.48.4 - Alternate Identifier
        obr_48_5 = obr_48_5  # OBR.48.5 - Alternate Text
        obr_48_6 = obr_48_6  # OBR.48.6 - Name Of Alternate Coding System
        obr_48_7 = obr_48_7  # OBR.48.7 - Coding System Version Id
        obr_48_8 = obr_48_8  # OBR.48.8 - Alternate Coding System Version Id
        obr_48_9 = obr_48_9  # OBR.48.9 - Original Text
        obr_49 = obr_49  # OBR.49 - Result Handling
        obr_50 = obr_50  # OBR.50 - Parent Universal Service Identifier
        obr_50_1 = obr_50_1  # OBR.50.1 - Identifier
        obr_50_2 = obr_50_2  # OBR.50.2 - Text
        obr_50_3 = obr_50_3  # OBR.50.3 - Name Of Coding System
        obr_50_4 = obr_50_4  # OBR.50.4 - Alternate Identifier
        obr_50_5 = obr_50_5  # OBR.50.5 - Alternate Text
        obr_50_6 = obr_50_6  # OBR.50.6 - Name Of Alternate Coding System
        obr_50_7 = obr_50_7  # OBR.50.7 - Coding System Version Id
        obr_50_8 = obr_50_8  # OBR.50.8 - Alternate Coding System Version Id
        obr_50_9 = obr_50_9  # OBR.50.9 - Original Text

        OBR = ()
        return OBR

    #This segment is defined here for inclusion in messages defined in other chapters. It is commonly used for sending notes and comments.
    def NTE(self):
        # Optional for V1.0
        pass

    #This segment is used to specify the complex timing of events and actions such as those that occur in order management and scheduling systems. 
    #This segment determines the quantity, frequency, priority, and timing of a service.
    def TQ1():
        #tq1_1 - Set ID - TQ1
        #tq1_2 - Quantity
        #tq1_2.1 - Quantity
        #tq1_2.2 - Units
        #tq1_2.2.1 - Identifier
        #tq1_2.2.2 - Text
        #tq1_2.2.3 - Name Of Coding System
        #tq1_2.2.4 - Alternate Identifier
        #tq1_2.2.5 - Alternate Text
        #tq1_2.2.6 - Name Of Alternate Coding System
        #tq1_3 - Repeat Pattern
        #tq1_3.1 - Repeat Pattern Code
        #tq1_3.1.1 - Identifier
        #tq1_3.1.2 - Text
        #tq1_3.1.3 - Name Of Coding System
        #tq1_3.1.4 - Alternate Identifier
        #tq1_3.1.5 - Alternate Text
        #tq1_3.1.6 - Name Of Alternate Coding System
        #tq1_3.1.7 - Coding System Version Id
        #tq1_3.1.8 - Alternate Coding System Version Id
        #tq1_3.1.9 - Original Text
        #tq1_3.2 - Calendar Alignment
        #tq1_3.3 - Phase Range Begin Value
        #tq1_3.4 - Phase Range End Value
        #tq1_3.5 - Period Quantity
        #tq1_3.6 - Period Units
        #tq1_3.7 - Institution Specified Time
        #tq1_3.8 - Event
        #tq1_3.9 - Event Offset Quantity
        #tq1_3.10 - Event Offset Units
        #tq1_3.11 - General Timing Specification
        #tq1_4 - Explicit Time
        #tq1_5 - Relative Time and Units
        #tq1_5.1 - Quantity
        #tq1_5.2 - Units
        #tq1_5.2.1 - Identifier
        #tq1_5.2.2 - Text
        #tq1_5.2.3 - Name Of Coding System
        #tq1_5.2.4 - Alternate Identifier
        #tq1_5.2.5 - Alternate Text
        #tq1_5.2.6 - Name Of Alternate Coding System
        #tq1_6 - Service Duration
        #tq1_6.1 - Quantity
        #tq1_6.2 - Units
        #tq1_6.2.1 - Identifier
        #tq1_6.2.2 - Text
        #tq1_6.2.3 - Name Of Coding System
        #tq1_6.2.4 - Alternate Identifier
        #tq1_6.2.5 - Alternate Text
        #tq1_6.2.6 - Name Of Alternate Coding System
        #tq1_7 - Start date/time
        #tq1_7.1 - Time
        #tq1_7.2 - Degree Of Precision
        #tq1_8 - End date/time
        #tq1_8.1 - Time
        #tq1_8.2 - Degree Of Precision
        #tq1_9 - Priority
        #tq1_9.1 - Identifier
        #tq1_9.2 - Text
        #tq1_9.3 - Name Of Coding System
        #tq1_9.4 - Alternate Identifier
        #tq1_9.5 - Alternate Text
        #tq1_9.6 - Name Of Alternate Coding System
        #tq1_9.7 - Coding System Version Id
        #tq1_9.8 - Alternate Coding System Version Id
        #tq1_9.9 - Original Text
        #tq1_10 - Condition text
        #tq1_11 - Text instruction
        #tq1_12 - Conjunction
        #tq1_13 - Occurrence duration
        #tq1_13.1 - Quantity
        #tq1_13.2 - Units
        #tq1_13.2.1 - Identifier
        #tq1_13.2.2 - Text
        #tq1_13.2.3 - Name Of Coding System
        #tq1_13.2.4 - Alternate Identifier
        #tq1_13.2.5 - Alternate Text
        #tq1_13.2.6 - Name Of Alternate Coding System
        #tq1_14 - Total occurrences

        TQ1 = ()
        return TQ1

    #This segment will link the current service request with one or more other service requests.
    def TQ2(self):
        # Optional for V1.0
        pass

    #This segment may identify any contact personnel associated with a patient referral message and its related transactions. The CTD segment will be paired with a PRD segment. The PRD segment contains data specifically focused on provider information in a referral.
    def CTD(self):
        # Optional for V1.0
        pass

    #This segment is used to transmit a single observation or observation fragment. It represents the smallest indivisible unit of a report. The OBX segment can also contain encapsulated data, e.g., a CDA document or a DICOM image.
    def OBX():
        #obx_1 - Set ID - OBX
        #obx_2 - Value Type
        #obx_3 - Observation Identifier
        #obx_3.1 - Identifier
        #obx_3.2 - Text
        #obx_3.3 - Name Of Coding System
        #obx_3.4 - Alternate Identifier
        #obx_3.5 - Alternate Text
        #obx_3.6 - Name Of Alternate Coding System
        #obx_4 - Observation Sub-ID
        #obx_5 - Observation Value
        #obx_6 - Units
        #obx_6.1 - Identifier
        #obx_6.2 - Text
        #obx_6.3 - Name Of Coding System
        #obx_6.4 - Alternate Identifier
        #obx_6.5 - Alternate Text
        #obx_6.6 - Name Of Alternate Coding System
        #obx_7 - References Range
        #obx_8 - Abnormal Flags
        #obx_9 - Probability
        #obx_10 - Nature of Abnormal Test
        #obx_11 - Observation Result Status
        #obx_12 - Effective Date of Reference Range
        #obx_12.1 - Time
        #obx_12.2 - Degree Of Precision
        #obx_13 - User Defined Access Checks
        #obx_14 - Date/Time of the Observation
        #obx_14.1 - Time
        #obx_14.2 - Degree Of Precision
        #obx_15 - Producer's ID
        #obx_15.1 - Identifier
        #obx_15.2 - Text
        #obx_15.3 - Name Of Coding System
        #obx_15.4 - Alternate Identifier
        #obx_15.5 - Alternate Text
        #obx_15.6 - Name Of Alternate Coding System
        #obx_16 - Responsible Observer
        #obx_16.1 - Id Number
        #obx_16.2 - Family Name
        #obx_16.2.1 - Surname
        #obx_16.2.2 - Own Surname Prefix
        #obx_16.2.3 - Own Surname
        #obx_16.2.4 - Surname Prefix From Partner/Spouse
        #obx_16.2.5 - Surname From Partner/Spouse
        #obx_16.3 - Given Name
        #obx_16.4 - Second And Further Given Names Or Initials Thereof
        #obx_16.5 - Suffix (e.g., Jr Or Iii)
        #obx_16.6 - Prefix (e.g., Dr)
        #obx_16.7 - Degree (e.g., Md)
        #obx_16.8 - Source Table
        #obx_16.9 - Assigning Authority
        #obx_16.9.1 - Namespace Id
        #obx_16.9.2 - Universal Id
        #obx_16.9.3 - Universal Id Type
        #obx_16.10 - Name Type Code
        #obx_16.11 - Identifier Check Digit
        #obx_16.12 - Check Digit Scheme
        #obx_16.13 - Identifier Type Code
        #obx_16.14 - Assigning Facility
        #obx_16.14.1 - Namespace Id
        #obx_16.14.2 - Universal Id
        #obx_16.14.3 - Universal Id Type
        #obx_16.15 - Name Representation Code
        #obx_16.16 - Name Context
        #obx_16.16.1 - Identifier
        #obx_16.16.2 - Text
        #obx_16.16.3 - Name Of Coding System
        #obx_16.16.4 - Alternate Identifier
        #obx_16.16.5 - Alternate Text
        #obx_16.16.6 - Name Of Alternate Coding System
        #obx_16.17 - Name Validity Range
        #obx_16.17.1 - Range Start Date/Time
        #obx_16.17.1.1 - Time
        #obx_16.17.1.2 - Degree Of Precision
        #obx_16.17.2 - Range End Date/Time
        #obx_16.17.2.1 - Time
        #obx_16.17.2.2 - Degree Of Precision
        #obx_16.18 - Name Assembly Order
        #obx_16.19 - Effective Date
        #obx_16.19.1 - Time
        #obx_16.19.2 - Degree Of Precision
        #obx_16.20 - Expiration Date
        #obx_16.20.1 - Time
        #obx_16.20.2 - Degree Of Precision
        #obx_16.21 - Professional Suffix
        #obx_16.22 - Assigning Jurisdiction
        #obx_16.22.1 - Identifier
        #obx_16.22.2 - Text
        #obx_16.22.3 - Name Of Coding System
        #obx_16.22.4 - Alternate Identifier
        #obx_16.22.5 - Alternate Text
        #obx_16.22.6 - Name Of Alternate Coding System
        #obx_16.22.7 - Coding System Version Id
        #obx_16.22.8 - Alternate Coding System Version Id
        #obx_16.22.9 - Original Text
        #obx_16.23 - Assigning Agency Or Department
        #obx_16.23.1 - Identifier
        #obx_16.23.2 - Text
        #obx_16.23.3 - Name Of Coding System
        #obx_16.23.4 - Alternate Identifier
        #obx_16.23.5 - Alternate Text
        #obx_16.23.6 - Name Of Alternate Coding System
        #obx_16.23.7 - Coding System Version Id
        #obx_16.23.8 - Alternate Coding System Version Id
        #obx_16.23.9 - Original Text
        #obx_17 - Observation Method
        #obx_17.1 - Identifier
        #obx_17.2 - Text
        #obx_17.3 - Name Of Coding System
        #obx_17.4 - Alternate Identifier
        #obx_17.5 - Alternate Text
        #obx_17.6 - Name Of Alternate Coding System
        #obx_18 - Equipment Instance Identifier
        #obx_18.1 - Entity Identifier
        #obx_18.2 - Namespace Id
        #obx_18.3 - Universal Id
        #obx_18.4 - Universal Id Type
        #obx_19 - Date/Time of the Analysis
        #obx_19.1 - Time
        #obx_19.2 - Degree Of Precision
        #obx_20 - Reserved for harmonization with V2.6
        #obx_21 - Reserved for harmonization with V2.6
        #obx_22 - Reserved for harmonization with V2.6
        #obx_23 - Performing Organization Name
        #obx_23.1 - Organization Name
        #obx_23.2 - Organization Name Type Code
        #obx_23.3 - Id Number
        #obx_23.4 - Check Digit
        #obx_23.5 - Check Digit Scheme
        #obx_23.6 - Assigning Authority
        #obx_23.6.1 - Namespace Id
        #obx_23.6.2 - Universal Id
        #obx_23.6.3 - Universal Id Type
        #obx_23.7 - Identifier Type Code
        #obx_23.8 - Assigning Facility
        #obx_23.8.1 - Namespace Id
        #obx_23.8.2 - Universal Id
        #obx_23.8.3 - Universal Id Type
        #obx_23.9 - Name Representation Code
        #obx_23.10 - Organization Identifier
        #obx_24 - Performing Organization Address
        #obx_24.1 - Street Address
        #obx_24.1.1 - Street Or Mailing Address
        #obx_24.1.2 - Street Name
        #obx_24.1.3 - Dwelling Number
        #obx_24.2 - Other Designation
        #obx_24.3 - City
        #obx_24.4 - State Or Province
        #obx_24.5 - Zip Or Postal Code
        #obx_24.6 - Country
        #obx_24.7 - Address Type
        #obx_24.8 - Other Geographic Designation
        #obx_24.9 - County/Parish Code
        #obx_24.10 - Census Tract
        #obx_24.11 - Address Representation Code
        #obx_24.12 - Address Validity Range
        #obx_24.12.1 - Range Start Date/Time
        #obx_24.12.1.1 - Time
        #obx_24.12.1.2 - Degree Of Precision
        #obx_24.12.2 - Range End Date/Time
        #obx_24.12.2.1 - Time
        #obx_24.12.2.2 - Degree Of Precision
        #obx_24.13 - Effective Date
        #obx_24.13.1 - Time
        #obx_24.13.2 - Degree Of Precision
        #obx_24.14 - Expiration Date
        #obx_24.14.1 - Time
        #obx_24.14.2 - Degree Of Precision
        #obx_25 - Performing Organization Medical Director
        #obx_25.1 - Id Number
        #obx_25.2 - Family Name
        #obx_25.2.1 - Surname
        #obx_25.2.2 - Own Surname Prefix
        #obx_25.2.3 - Own Surname
        #obx_25.2.4 - Surname Prefix From Partner/Spouse
        #obx_25.2.5 - Surname From Partner/Spouse
        #obx_25.3 - Given Name
        #obx_25.4 - Second And Further Given Names Or Initials Thereof
        #obx_25.5 - Suffix (e.g., Jr Or Iii)
        #obx_25.6 - Prefix (e.g., Dr)
        #obx_25.7 - Degree (e.g., Md)
        #obx_25.8 - Source Table
        #obx_25.9 - Assigning Authority
        #obx_25.9.1 - Namespace Id
        #obx_25.9.2 - Universal Id
        #obx_25.9.3 - Universal Id Type
        #obx_25.10 - Name Type Code
        #obx_25.11 - Identifier Check Digit
        #obx_25.12 - Check Digit Scheme
        #obx_25.13 - Identifier Type Code
        #obx_25.14 - Assigning Facility
        #obx_25.14.1 - Namespace Id
        #obx_25.14.2 - Universal Id
        #obx_25.14.3 - Universal Id Type
        #obx_25.15 - Name Representation Code
        #obx_25.16 - Name Context
        #obx_25.16.1 - Identifier
        #obx_25.16.2 - Text
        #obx_25.16.3 - Name Of Coding System
        #obx_25.16.4 - Alternate Identifier
        #obx_25.16.5 - Alternate Text
        #obx_25.16.6 - Name Of Alternate Coding System
        #obx_25.17 - Name Validity Range
        #obx_25.17.1 - Range Start Date/Time
        #obx_25.17.1.1 - Time
        #obx_25.17.1.2 - Degree Of Precision
        #obx_25.17.2 - Range End Date/Time
        #obx_25.17.2.1 - Time
        #obx_25.17.2.2 - Degree Of Precision
        #obx_25.18 - Name Assembly Order
        #obx_25.19 - Effective Date
        #obx_25.19.1 - Time
        #obx_25.19.2 - Degree Of Precision
        #obx_25.20 - Expiration Date
        #obx_25.20.1 - Time
        #obx_25.20.2 - Degree Of Precision
        #obx_25.21 - Professional Suffix
        #obx_25.22 - Assigning Jurisdiction
        #obx_25.22.1 - Identifier
        #obx_25.22.2 - Text
        #obx_25.22.3 - Name Of Coding System
        #obx_25.22.4 - Alternate Identifier
        #obx_25.22.5 - Alternate Text
        #obx_25.22.6 - Name Of Alternate Coding System
        #obx_25.22.7 - Coding System Version Id
        #obx_25.22.8 - Alternate Coding System Version Id
        #obx_25.22.9 - Original Text
        #obx_25.23 - Assigning Agency Or Department
        #obx_25.23.1 - Identifier
        #obx_25.23.2 - Text
        #obx_25.23.3 - Name Of Coding System
        #obx_25.23.4 - Alternate Identifier
        #obx_25.23.5 - Alternate Text
        #obx_25.23.6 - Name Of Alternate Coding System
        #obx_25.23.7 - Coding System Version Id
        #obx_25.23.8 - Alternate Coding System Version Id
        #obx_25.23.9 - Original Text

        OBX = ()
        return OBX

    #This segment is commonly used for sending notes and comments.
    def NTE(self):
        # Optional for V1.0
        pass

    #This segment contains the detail data necessary to post charges, payments, adjustments, etc. to patient accounting records.
    def FT1(self):
        # Optional for V1.0
        pass

    #This segment is an optional segment that contains information to identify the clinical trial, phase and time point with which an order or result is associated.
    def CTI(self):
        # Optional for V1.0
        pass

    #This segment is to describe the characteristics of a specimen and generalizes the multiple relationships among order(s), results, specimen(s) and specimen container(s).
    def SPM():
        #spm_1 - Set ID - SPM
        #spm_2 - Specimen ID
        #spm_2.1 - Placer Assigned Identifier
        #spm_2.1.1 - Entity Identifier
        #spm_2.1.2 - Namespace Id
        #spm_2.1.3 - Universal Id
        #spm_2.1.4 - Universal Id Type
        #spm_2.2 - Filler Assigned Identifier
        #spm_2.2.1 - Entity Identifier
        #spm_2.2.2 - Namespace Id
        #spm_2.2.3 - Universal Id
        #spm_2.2.4 - Universal Id Type
        #spm_3 - Specimen Parent IDs
        #spm_3.1 - Placer Assigned Identifier
        #spm_3.1.1 - Entity Identifier
        #spm_3.1.2 - Namespace Id
        #spm_3.1.3 - Universal Id
        #spm_3.1.4 - Universal Id Type
        #spm_3.2 - Filler Assigned Identifier
        #spm_3.2.1 - Entity Identifier
        #spm_3.2.2 - Namespace Id
        #spm_3.2.3 - Universal Id
        #spm_3.2.4 - Universal Id Type
        #spm_4 - Specimen Type
        #spm_4.1 - Identifier
        #spm_4.2 - Text
        #spm_4.3 - Name Of Coding System
        #spm_4.4 - Alternate Identifier
        #spm_4.5 - Alternate Text
        #spm_4.6 - Name Of Alternate Coding System
        #spm_4.7 - Coding System Version Id
        #spm_4.8 - Alternate Coding System Version Id
        #spm_4.9 - Original Text
        #spm_5 - Specimen Type Modifier
        #spm_5.1 - Identifier
        #spm_5.2 - Text
        #spm_5.3 - Name Of Coding System
        #spm_5.4 - Alternate Identifier
        #spm_5.5 - Alternate Text
        #spm_5.6 - Name Of Alternate Coding System
        #spm_5.7 - Coding System Version Id
        #spm_5.8 - Alternate Coding System Version Id
        #spm_5.9 - Original Text
        #spm_6 - Specimen Additives
        #spm_6.1 - Identifier
        #spm_6.2 - Text
        #spm_6.3 - Name Of Coding System
        #spm_6.4 - Alternate Identifier
        #spm_6.5 - Alternate Text
        #spm_6.6 - Name Of Alternate Coding System
        #spm_6.7 - Coding System Version Id
        #spm_6.8 - Alternate Coding System Version Id
        #spm_6.9 - Original Text
        #spm_7 - Specimen Collection Method
        #spm_7.1 - Identifier
        #spm_7.2 - Text
        #spm_7.3 - Name Of Coding System
        #spm_7.4 - Alternate Identifier
        #spm_7.5 - Alternate Text
        #spm_7.6 - Name Of Alternate Coding System
        #spm_7.7 - Coding System Version Id
        #spm_7.8 - Alternate Coding System Version Id
        #spm_7.9 - Original Text
        #spm_8 - Specimen Source Site
        #spm_8.1 - Identifier
        #spm_8.2 - Text
        #spm_8.3 - Name Of Coding System
        #spm_8.4 - Alternate Identifier
        #spm_8.5 - Alternate Text
        #spm_8.6 - Name Of Alternate Coding System
        #spm_8.7 - Coding System Version Id
        #spm_8.8 - Alternate Coding System Version Id
        #spm_8.9 - Original Text
        #spm_9 - Specimen Source Site Modifier
        #spm_9.1 - Identifier
        #spm_9.2 - Text
        #spm_9.3 - Name Of Coding System
        #spm_9.4 - Alternate Identifier
        #spm_9.5 - Alternate Text
        #spm_9.6 - Name Of Alternate Coding System
        #spm_9.7 - Coding System Version Id
        #spm_9.8 - Alternate Coding System Version Id
        #spm_9.9 - Original Text
        #spm_10 - Specimen Collection Site
        #spm_10.1 - Identifier
        #spm_10.2 - Text
        #spm_10.3 - Name Of Coding System
        #spm_10.4 - Alternate Identifier
        #spm_10.5 - Alternate Text
        #spm_10.6 - Name Of Alternate Coding System
        #spm_10.7 - Coding System Version Id
        #spm_10.8 - Alternate Coding System Version Id
        #spm_10.9 - Original Text
        #spm_11 - Specimen Role
        #spm_11.1 - Identifier
        #spm_11.2 - Text
        #spm_11.3 - Name Of Coding System
        #spm_11.4 - Alternate Identifier
        #spm_11.5 - Alternate Text
        #spm_11.6 - Name Of Alternate Coding System
        #spm_11.7 - Coding System Version Id
        #spm_11.8 - Alternate Coding System Version Id
        #spm_11.9 - Original Text
        #spm_12 - Specimen Collection Amount
        #spm_12.1 - Quantity
        #spm_12.2 - Units
        #spm_12.2.1 - Identifier
        #spm_12.2.2 - Text
        #spm_12.2.3 - Name Of Coding System
        #spm_12.2.4 - Alternate Identifier
        #spm_12.2.5 - Alternate Text
        #spm_12.2.6 - Name Of Alternate Coding System
        #spm_13 - Grouped Specimen Count
        #spm_14 - Specimen Description
        #spm_15 - Specimen Handling Code
        #spm_15.1 - Identifier
        #spm_15.2 - Text
        #spm_15.3 - Name Of Coding System
        #spm_15.4 - Alternate Identifier
        #spm_15.5 - Alternate Text
        #spm_15.6 - Name Of Alternate Coding System
        #spm_15.7 - Coding System Version Id
        #spm_15.8 - Alternate Coding System Version Id
        #spm_15.9 - Original Text
        #spm_16 - Specimen Risk Code
        #spm_16.1 - Identifier
        #spm_16.2 - Text
        #spm_16.3 - Name Of Coding System
        #spm_16.4 - Alternate Identifier
        #spm_16.5 - Alternate Text
        #spm_16.6 - Name Of Alternate Coding System
        #spm_16.7 - Coding System Version Id
        #spm_16.8 - Alternate Coding System Version Id
        #spm_16.9 - Original Text
        #spm_17 - Specimen Collection Date/Time
        #spm_17.1 - Range Start Date/Time
        #spm_17.1.1 - Time
        #spm_17.1.2 - Degree Of Precision
        #spm_17.2 - Range End Date/Time
        #spm_17.2.1 - Time
        #spm_17.2.2 - Degree Of Precision
        #spm_18 - Specimen Received Date/Time
        #spm_18.1 - Time
        #spm_18.2 - Degree Of Precision
        #spm_19 - Specimen Expiration Date/Time
        #spm_19.1 - Time
        #spm_19.2 - Degree Of Precision
        #spm_20 - Specimen Availability
        #spm_21 - Specimen Reject Reason
        #spm_21.1 - Identifier
        #spm_21.2 - Text
        #spm_21.3 - Name Of Coding System
        #spm_21.4 - Alternate Identifier
        #spm_21.5 - Alternate Text
        #spm_21.6 - Name Of Alternate Coding System
        #spm_21.7 - Coding System Version Id
        #spm_21.8 - Alternate Coding System Version Id
        #spm_21.9 - Original Text
        #spm_22 - Specimen Quality
        #spm_22.1 - Identifier
        #spm_22.2 - Text
        #spm_22.3 - Name Of Coding System
        #spm_22.4 - Alternate Identifier
        #spm_22.5 - Alternate Text
        #spm_22.6 - Name Of Alternate Coding System
        #spm_22.7 - Coding System Version Id
        #spm_22.8 - Alternate Coding System Version Id
        #spm_22.9 - Original Text
        #spm_23 - Specimen Appropriateness
        #spm_23.1 - Identifier
        #spm_23.2 - Text
        #spm_23.3 - Name Of Coding System
        #spm_23.4 - Alternate Identifier
        #spm_23.5 - Alternate Text
        #spm_23.6 - Name Of Alternate Coding System
        #spm_23.7 - Coding System Version Id
        #spm_23.8 - Alternate Coding System Version Id
        #spm_23.9 - Original Text
        #spm_24 - Specimen Condition
        #spm_24.1 - Identifier
        #spm_24.2 - Text
        #spm_24.3 - Name Of Coding System
        #spm_24.4 - Alternate Identifier
        #spm_24.5 - Alternate Text
        #spm_24.6 - Name Of Alternate Coding System
        #spm_24.7 - Coding System Version Id
        #spm_24.8 - Alternate Coding System Version Id
        #spm_24.9 - Original Text
        #spm_25 - Specimen Current Quantity
        #spm_25.1 - Quantity
        #spm_25.2 - Units
        #spm_25.2.1 - Identifier
        #spm_25.2.2 - Text
        #spm_25.2.3 - Name Of Coding System
        #spm_25.2.4 - Alternate Identifier
        #spm_25.2.5 - Alternate Text
        #spm_25.2.6 - Name Of Alternate Coding System
        #spm_26 - Number of Specimen Containers
        #spm_27 - Container Type
        #spm_27.1 - Identifier
        #spm_27.2 - Text
        #spm_27.3 - Name Of Coding System
        #spm_27.4 - Alternate Identifier
        #spm_27.5 - Alternate Text
        #spm_27.6 - Name Of Alternate Coding System
        #spm_27.7 - Coding System Version Id
        #spm_27.8 - Alternate Coding System Version Id
        #spm_27.9 - Original Text
        #spm_28 - Container Condition
        #spm_28.1 - Identifier
        #spm_28.2 - Text
        #spm_28.3 - Name Of Coding System
        #spm_28.4 - Alternate Identifier
        #spm_28.5 - Alternate Text
        #spm_28.6 - Name Of Alternate Coding System
        #spm_28.7 - Coding System Version Id
        #spm_28.8 - Alternate Coding System Version Id
        #spm_28.9 - Original Text
        #spm_29 - Specimen Child Role
        #spm_29.1 - Identifier
        #spm_29.2 - Text
        #spm_29.3 - Name Of Coding System
        #spm_29.4 - Alternate Identifier
        #spm_29.5 - Alternate Text
        #spm_29.6 - Name Of Alternate Coding System
        #spm_29.7 - Coding System Version Id
        #spm_29.8 - Alternate Coding System Version Id
        #spm_29.9 - Original Text

        SPM = ()
        return SPM

    #This segment is used to transmit a single observation or observation fragment. 
    def OBX(self):
        # Optional for V1.0
        pass

    #This segment is used in the continuation protocol.
    def DSC(self):
        # Optional for V1.0
        pass
