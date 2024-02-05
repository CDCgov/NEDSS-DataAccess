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
        #obr_1 - Set ID - OBR
        #obr_2 - Placer Order Number
        #obr_2_1 - Entity Identifier
        #obr_2_2 - Namespace Id
        #obr_2_3 - Universal Id
        #obr_2_4 - Universal Id Type
        #obr_3 - Filler Order Number
        #obr_3.1 - Entity Identifier
        #obr_3.2 - Namespace Id
        #obr_3.3 - Universal Id
        #obr_3.4 - Universal Id Type
        #obr_4 - Universal Service Identifier
        #obr_4.1 - Identifier
        #obr_4.2 - Text
        #obr_4.3 - Name Of Coding System
        #obr_4.4 - Alternate Identifier
        #obr_4.5 - Alternate Text
        #obr_4.6 - Name Of Alternate Coding System
        #obr_5 - Priority - OBR
        #obr_6 - Requested Date/Time
        #obr_6.1 - Time
        #obr_6.2 - Degree Of Precision
        #obr_7 - Observation Date/Time
        #obr_7.1 - Time
        #obr_7.2 - Degree Of Precision
        #obr_8 - Observation End Date/Time
        #obr_8.1 - Time
        #obr_8.2 - Degree Of Precision
        #obr_9 - Collection Volume
        #obr_9.1 - Quantity
        #obr_9.2 - Units
        #obr_10 - Collector Identifier
        #obr_10.1 - Id Number
        #obr_10.2 - Family Name
        #obr_10.2.1 - Surname
        #obr_10.2.2 - Own Surname Prefix
        #obr_10.2.3 - Own Surname
        #obr_10.2.4 - Surname Prefix From Partner/Spouse
        #obr_10.2.5 - Surname From Partner/Spouse
        #obr_10.3 - Given Name
        #obr_10.4 - Second And Further Given Names Or Initials Thereof
        #obr_10.5 - Suffix (e.g., Jr Or Iii)
        #obr_10.6 - Prefix (e.g., Dr)
        #obr_10.7 - Degree (e.g., Md)
        #obr_10.8 - Source Table
        #obr_10.9 - Assigning Authority
        #obr_10.9.1 - Namespace Id
        #obr_10.9.2 - Universal Id
        #obr_10.9.3 - Universal Id Type
        #obr_10.10 - Name Type Code
        #obr_10.11 - Identifier Check Digit
        #obr_10.12 - Check Digit Scheme
        #obr_10.13 - Identifier Type Code
        #obr_10.14 - Assigning Facility
        #obr_10.14.1 - Namespace Id
        #obr_10.14.2 - Universal Id
        #obr_10.14.3 - Universal Id Type
        #obr_10.15 - Name Representation Code
        #obr_10.16 - Name Context
        #obr_10.16.1 - Identifier
        #obr_10.16.2 - Text
        #obr_10.16.3 - Name Of Coding System
        #obr_10.16.4 - Alternate Identifier
        #obr_10.16.5 - Alternate Text
        #obr_10.16.6 - Name Of Alternate Coding System
        #obr_10.17 - Name Validity Range
        #obr_10.17.1 - Range Start Date/Time
        #obr_10.17.1.1 - Time
        #obr_10.17.1.2 - Degree Of Precision
        #obr_10.17.2 - Range End Date/Time
        #obr_10.17.2.1 - Time
        #obr_10.17.2.2 - Degree Of Precision
        #obr_10.18 - Name Assembly Order
        #obr_10.19 - Effective Date
        #obr_10.19.1 - Time
        #obr_10.19.2 - Degree Of Precision
        #obr_10.20 - Expiration Date
        #obr_10.20.1 - Time
        #obr_10.20.2 - Degree Of Precision
        #obr_10.21 - Professional Suffix
        #obr_10.22 - Assigning Jurisdiction
        #obr_10.22.1 - Identifier
        #obr_10.22.2 - Text
        #obr_10.22.3 - Name Of Coding System
        #obr_10.22.4 - Alternate Identifier
        #obr_10.22.5 - Alternate Text
        #obr_10.22.6 - Name Of Alternate Coding System
        #obr_10.22.7 - Coding System Version Id
        #obr_10.22.8 - Alternate Coding System Version Id
        #obr_10.22.9 - Original Text
        #obr_10.23 - Assigning Agency Or Department
        #obr_10.23.1 - Identifier
        #obr_10.23.2 - Text
        #obr_10.23.3 - Name Of Coding System
        #obr_10.23.4 - Alternate Identifier
        #obr_10.23.5 - Alternate Text
        #obr_10.23.6 - Name Of Alternate Coding System
        #obr_10.23.7 - Coding System Version Id
        #obr_10.23.8 - Alternate Coding System Version Id
        #obr_10.23.9 - Original Text
        #obr_11 - Specimen Action Code
        #obr_12 - Danger Code
        #obr_12.1 - Identifier
        #obr_12.2 - Text
        #obr_12.3 - Name Of Coding System
        #obr_12.4 - Alternate Identifier
        #obr_12.5 - Alternate Text
        #obr_12.6 - Name Of Alternate Coding System
        #obr_13 - Relevant Clinical Information
        #obr_14 - Specimen Received Date/Time
        #obr_14.1 - Time
        #obr_14.2 - Degree Of Precision
        #obr_15 - Specimen Source
        #obr_15.1 - Specimen Source Name Or Code
        #obr_15.1.1 - Identifier
        #obr_15.1.2 - Text
        #obr_15.1.3 - Name Of Coding System
        #obr_15.1.4 - Alternate Identifier
        #obr_15.1.5 - Alternate Text
        #obr_15.1.6 - Name Of Alternate Coding System
        #obr_15.1.7 - Coding System Version Id
        #obr_15.1.8 - Alternate Coding System Version Id
        #obr_15.1.9 - Original Text
        #obr_15.2 - Additives
        #obr_15.2.1 - Identifier
        #obr_15.2.2 - Text
        #obr_15.2.3 - Name Of Coding System
        #obr_15.2.4 - Alternate Identifier
        #obr_15.2.5 - Alternate Text
        #obr_15.2.6 - Name Of Alternate Coding System
        #obr_15.2.7 - Coding System Version Id
        #obr_15.2.8 - Alternate Coding System Version Id
        #obr_15.2.9 - Original Text
        #obr_15.3 - Specimen Collection Method
        #obr_15.4 - Body Site
        #obr_15.4.1 - Identifier
        #obr_15.4.2 - Text
        #obr_15.4.3 - Name Of Coding System
        #obr_15.4.4 - Alternate Identifier
        #obr_15.4.5 - Alternate Text
        #obr_15.4.6 - Name Of Alternate Coding System
        #obr_15.4.7 - Coding System Version Id
        #obr_15.4.8 - Alternate Coding System Version Id
        #obr_15.4.9 - Original Text
        #obr_15.5 - Site Modifier
        #obr_15.5.1 - Identifier
        #obr_15.5.2 - Text
        #obr_15.5.3 - Name Of Coding System
        #obr_15.5.4 - Alternate Identifier
        #obr_15.5.5 - Alternate Text
        #obr_15.5.6 - Name Of Alternate Coding System
        #obr_15.5.7 - Coding System Version Id
        #obr_15.5.8 - Alternate Coding System Version Id
        #obr_15.5.9 - Original Text
        #obr_15.6 - Collection Method Modifier Code
        #obr_15.6.1 - Identifier
        #obr_15.6.2 - Text
        #obr_15.6.3 - Name Of Coding System
        #obr_15.6.4 - Alternate Identifier
        #obr_15.6.5 - Alternate Text
        #obr_15.6.6 - Name Of Alternate Coding System
        #obr_15.6.7 - Coding System Version Id
        #obr_15.6.8 - Alternate Coding System Version Id
        #obr_15.6.9 - Original Text
        #obr_15.7 - Specimen Role
        #obr_15.7.1 - Identifier
        #obr_15.7.2 - Text
        #obr_15.7.3 - Name Of Coding System
        #obr_15.7.4 - Alternate Identifier
        #obr_15.7.5 - Alternate Text
        #obr_15.7.6 - Name Of Alternate Coding System
        #obr_15.7.7 - Coding System Version Id
        #obr_15.7.8 - Alternate Coding System Version Id
        #obr_15.7.9 - Original Text
        #obr_16 - Ordering Provider
        #obr_16.1 - Id Number
        #obr_16.2 - Family Name
        #obr_16.2.1 - Surname
        #obr_16.2.2 - Own Surname Prefix
        #obr_16.2.3 - Own Surname
        #obr_16.2.4 - Surname Prefix From Partner/Spouse
        #obr_16.2.5 - Surname From Partner/Spouse
        #obr_16.3 - Given Name
        #obr_16.4 - Second And Further Given Names Or Initials Thereof
        #obr_16.5 - Suffix (e.g., Jr Or Iii)
        #obr_16.6 - Prefix (e.g., Dr)
        #obr_16.7 - Degree (e.g., Md)
        #obr_16.8 - Source Table
        #obr_16.9 - Assigning Authority
        #obr_16.9.1 - Namespace Id
        #obr_16.9.2 - Universal Id
        #obr_16.9.3 - Universal Id Type
        #obr_16.10 - Name Type Code
        #obr_16.11 - Identifier Check Digit
        #obr_16.12 - Check Digit Scheme
        #obr_16.13 - Identifier Type Code
        #obr_16.14 - Assigning Facility
        #obr_16.14.1 - Namespace Id
        #obr_16.14.2 - Universal Id
        #obr_16.14.3 - Universal Id Type
        #obr_16.15 - Name Representation Code
        #obr_16.16 - Name Context
        #obr_16.16.1 - Identifier
        #obr_16.16.2 - Text
        #obr_16.16.3 - Name Of Coding System
        #obr_16.16.4 - Alternate Identifier
        #obr_16.16.5 - Alternate Text
        #obr_16.16.6 - Name Of Alternate Coding System
        #obr_16.17 - Name Validity Range
        #obr_16.17.1 - Range Start Date/Time
        #obr_16.17.1.1 - Time
        #obr_16.17.1.2 - Degree Of Precision
        #obr_16.17.2 - Range End Date/Time
        #obr_16.17.2.1 - Time
        #obr_16.17.2.2 - Degree Of Precision
        #obr_16.18 - Name Assembly Order
        #obr_16.19 - Effective Date
        #obr_16.19.1 - Time
        #obr_16.19.1 - Degree Of Precision
        #obr_16.20 - Expiration Date
        #obr_16.20.1 - Time
        #obr_16.20.1 - Degree Of Precision        
        #obr_16.21 - Professional Suffix
        #obr_16.22 - Assigning Jurisdiction
        #obr_16.22.1 - Identifier
        #obr_16.22.2 - Text
        #obr_16.22.3 - Name Of Coding System
        #obr_16.22.4 - Alternate Identifier
        #obr_16.22.5 - Alternate Text
        #obr_16.22.6 - Name Of Alternate Coding System
        #obr_16.22.7 - Coding System Version Id
        #obr_16.22.8 - Alternate Coding System Version Id
        #obr_16.22.9 - Original Text
        #obr_16.23 - Assigning Agency Or Department
        #obr_16.23.1 - Identifier
        #obr_16.23.2 - Text
        #obr_16.23.3 - Name Of Coding System
        #obr_16.23.4 - Alternate Identifier
        #obr_16.23.5 - Alternate Text
        #obr_16.23.6 - Name Of Alternate Coding System
        #obr_16.23.7 - Coding System Version Id
        #obr_16.23.8 - Alternate Coding System Version Id
        #obr_16.23.9 - Original Text
        #obr_17 - Order Callback Phone Number
        #obr_17.1 - Telephone Number
        #obr_17.2 - Telecommunication Use Code
        #obr_17.3 - Telecommunication Equipment Type
        #obr_17.4 - Email Address
        #obr_17.5 - Country Code
        #obr_17.6 - Area/City Code
        #obr_17.7 - Local Number
        #obr_17.8 - Extension
        #obr_17.9 - Any Text
        #obr_17.10 - Extension Prefix
        #obr_17.11 - Speed Dial Code
        #obr_17.12 - Unformatted Telephone Number
        #obr_18 - Placer Field 1
        #obr_19 - Placer Field 2
        #obr_20 - Filler Field 1
        #obr_21 - Filler Field 2
        #obr_22 - Results Rpt/Status Chng - Date/Time
        #obr_22.1 - Time
        #obr_22.1 - Degree Of Precision
        #obr_23 - Charge to Practice
        #obr_23.1 - Monetary Amount
        #obr_23.1.1 - Quantity
        #obr_23.1.2 - Denomination
        #obr_23.2 - Charge Code
        #obr_23.2.1 - Identifier
        #obr_23.2.2 - Text
        #obr_23.2.3 - Name Of Coding System
        #obr_23.2.4 - Alternate Identifier
        #obr_23.2.5 - Alternate Text
        #obr_23.2.6 - Name Of Alternate Coding System
        #obr_24 - Diagnostic Serv Sect ID
        #obr_25 - Result Status
        #obr_26 - Parent Result
        #obr_26.1 - Parent Observation Identifier
        #obr_26.1.1 - Identifier
        #obr_26.1.2 - Text
        #obr_26.1.3 - Name Of Coding System
        #obr_26.1.4 - Alternate Identifier
        #obr_26.1.5 - Alternate Text
        #obr_26.1.6 - Name Of Alternate Coding System
        #obr_26.2 - Parent Observation Sub-identifier
        #obr_26.3 - Parent Observation Value Descriptor
        #obr_27 - Quantity/Timing
        #obr_27.1 - Quantity
        #obr_27.1.1 - Quantity
        #obr_27.1.2 - Units
        #obr_27.1.1 - Identifier
        #obr_27.1.2 - Text
        #obr_27.1.3 - Name Of Coding System
        #obr_27.1.4 - Alternate Identifier
        #obr_27.1.5 - Alternate Text
        #obr_27.1.6 - Name Of Alternate Coding System
        #obr_27.2 - Interval
        #obr_27.2.1 - Repeat Pattern
        #obr_27.2.2 - Explicit Time Interval
        #obr_27.3 - Duration
        #obr_27.4 - Start Date/Time
        #obr_27.4.1 - Time
        #obr_27.4.2 - Degree Of Precision
        #obr_27.5 - End Date/Time
        #obr_27.5.1 - Time
        #obr_27.5.2 - Degree Of Precision
        #obr_27.6 - Priority
        #obr_27.7 - Condition
        #obr_27.8 - Text
        #obr_27.9 - Conjunction
        #obr_27.10 - Order Sequencing
        #obr_27.10.1 - Sequence/Results Flag
        #obr_27.10.2 - Placer Order Number: Entity Identifier
        #obr_27.10.3 - Placer Order Number: Namespace Id
        #obr_27.10.4 - Filler Order Number: Entity Identifier
        #obr_27.10.5 - Filler Order Number: Namespace Id
        #obr_27.10.6 - Sequence Condition Value
        #obr_27.10.7 - Maximum Number Of Repeats
        #obr_27.10.8 - Placer Order Number: Universal Id
        #obr_27.10.9 - Placer Order Number: Universal Id Type
        #obr_27.10.10 - Filler Order Number: Universal Id
        #obr_27.10.11 - Filler Order Number: Universal Id Type
        #obr_27.11 - Occurrence Duration
        #obr_27.11.1 - Identifier
        #obr_27.11.2 - Text
        #obr_27.11.3 - Name Of Coding System
        #obr_27.11.4 - Alternate Identifier
        #obr_27.11.5 - Alternate Text
        #obr_27.11.6 - Name Of Alternate Coding System
        #obr_27.12 - Total Occurrences
        #obr_28 - Result Copies To
        #obr_28.1 - Id Number
        #obr_28.2 - Family Name
        #obr_28.2.1 - Surname
        #obr_28.2.2 - Own Surname Prefix
        #obr_28.2.3 - Own Surname
        #obr_28.2.4 - Surname Prefix From Partner/Spouse
        #obr_28.2.5 - Surname From Partner/Spouse
        #obr_28.3 - Given Name
        #obr_28.4 - Second And Further Given Names Or Initials Thereof
        #obr_28.5 - Suffix (e.g., Jr Or Iii)
        #obr_28.6 - Prefix (e.g., Dr)
        #obr_28.7 - Degree (e.g., Md)
        #obr_28.8 - Source Table
        #obr_28.9 - Assigning Authority
        #obr_28.9.1 - Namespace Id
        #obr_28.9.2 - Universal Id
        #obr_28.9.3 - Universal Id Type
        #obr_28.10 - Name Type Code
        #obr_28.11 - Identifier Check Digit
        #obr_28.12 - Check Digit Scheme
        #obr_28.13 - Identifier Type Code
        #obr_28.14 - Assigning Facility
        #obr_28.14.1 - Namespace Id
        #obr_28.14.2 - Universal Id
        #obr_28.14.3 - Universal Id Type
        #obr_28.15 - Name Representation Code
        #obr_28.16 - Name Context
        #obr_28.16.1 - Identifier
        #obr_28.16.2 - Text
        #obr_28.16.3 - Name Of Coding System
        #obr_28.16.4 - Alternate Identifier
        #obr_28.16.5 - Alternate Text
        #obr_28.16.6 - Name Of Alternate Coding System
        #obr_28.17 - Name Validity Range
        #obr_28.17.1 - Range Start Date/Time
        #obr_28.17.1.1 - Time
        #obr_28.17.1.2 - Degree Of Precision
        #obr_28.17.2 - Range End Date/Time
        #obr_28.17.2.1 - Time
        #obr_28.17.2.2 - Degree Of Precision
        #obr_28.18 - Name Assembly Order
        #obr_28.19 - Effective Date
        #obr_28.19.1 - Time
        #obr_28.19.2 - Degree Of Precision
        #obr_28.20 - Expiration Date
        #obr_28.20.1 - Time
        #obr_28.20.2 - Degree Of Precision
        #obr_28.21 - Professional Suffix
        #obr_28.22 - Assigning Jurisdiction
        #obr_28.22.1 - Identifier
        #obr_28.22.2 - Text
        #obr_28.22.3 - Name Of Coding System
        #obr_28.22.4 - Alternate Identifier
        #obr_28.22.5 - Alternate Text
        #obr_28.22.6 - Name Of Alternate Coding System
        #obr_28.22.7 - Coding System Version Id
        #obr_28.22.8 - Alternate Coding System Version Id
        #obr_28.22.9 - Original Text
        #obr_28.23 - Assigning Agency Or Department
        #obr_28.23.1 - Identifier
        #obr_28.23.2 - Text
        #obr_28.23.3 - Name Of Coding System
        #obr_28.23.4 - Alternate Identifier
        #obr_28.23.5 - Alternate Text
        #obr_28.23.6 - Name Of Alternate Coding System
        #obr_28.23.7 - Coding System Version Id
        #obr_28.23.8 - Alternate Coding System Version Id
        #obr_28.23.9 - Original Text
        #obr_29 - Parent
        #obr_29.1 - Placer Assigned Identifier
        #obr_29.1.1 - Entity Identifier
        #obr_29.1.2 - Namespace Id
        #obr_29.1.3 - Universal Id
        #obr_29.1.4 - Universal Id Type
        #obr_29.2 - Filler Assigned Identifier
        #obr_29.2.1 - Entity Identifier
        #obr_29.2.2 - Namespace Id
        #obr_29.2.3 - Universal Id
        #obr_29.2.4 - Universal Id Type
        #obr_30 - Transportation Mode
        #obr_31 - Reason for Study
        #obr_31.1 - Identifier
        #obr_31.2 - Text
        #obr_31.3 - Name Of Coding System
        #obr_31.4 - Alternate Identifier
        #obr_31.5 - Alternate Text
        #obr_31.6 - Name Of Alternate Coding System
        #obr_32 - Principal Result Interpreter
        #obr_32.1 - Name
        #obr_32.1.1 - Id Number
        #obr_32.1.2 - Family Name
        #obr_32.1.3 - Given Name
        #obr_32.1.4 - Second And Further Given Names Or Initials Thereof
        #obr_32.1.5 - Suffix
        #obr_32.1.6 - Prefix
        #obr_32.1.7 - Degree
        #obr_32.1.8 - Source Table
        #obr_32.1.9 - Assigning Authority - Namespace Id
        #obr_32.1.10 - Assigning Authority- Universal Id
        #obr_32.1.11 - Assigning Authority - Universal Id Type
        #obr_32.2 - Start Date/Time
        #obr_32.2.1 - Time
        #obr_32.2.2 - Degree Of Precision
        #obr_32.3 - End Date/Time
        #obr_32.3.1 - Time
        #obr_32.3.2 - Degree Of Precision
        #obr_32.4 - Point Of Care
        #obr_32.5 - Room
        #obr_32.6 - Bed
        #obr_32.7 - Facility
        #obr_32.7.1 - Namespace Id
        #obr_32.7.2 - Universal Id
        #obr_32.7.3 - Universal Id Type
        #obr_32.8 - Location Status
        #obr_32.9 - Patient Location Type
        #obr_32.10 - Building
        #obr_32.11 - Floor
        #obr_33 - Assistant Result Interpreter
        #obr_33.1 - Name
        #obr_33.1.1 - Id Number
        #obr_33.1.2 - Family Name
        #obr_33.1.3 - Given Name
        #obr_33.1.4 - Second And Further Given Names Or Initials Thereof
        #obr_33.1.5 - Suffix
        #obr_33.1.6 - Prefix
        #obr_33.1.7 - Degree
        #obr_33.1.8 - Source Table
        #obr_33.1.9 - Assigning Authority - Namespace Id
        #obr_33.1.10 - Assigning Authority- Universal Id
        #obr_33.1.11 - Assigning Authority - Universal Id Type
        #obr_33.2 - Start Date/Time
        #obr_33.2.1 - Time
        #obr_33.2.2 - Degree Of Precision
        #obr_33.3 - End Date/Time
        #obr_33.3.1 - Time
        #obr_33.3.2 - Degree Of Precision
        #obr_33.4 - Point Of Care
        #obr_33.5 - Room
        #obr_33.6 - Bed
        #obr_33.7 - Facility
        #obr_33.7.1 - Namespace Id
        #obr_33.7.2 - Universal Id
        #obr_33.7.3 - Universal Id Type
        #obr_33.8 - Location Status
        #obr_33.9 - Patient Location Type
        #obr_33.10 - Building
        #obr_33.11 - Floor
        #obr_34 - Technician
        #obr_34.1 - Name
        #obr_34.1.1 - Id Number
        #obr_34.1.2 - Family Name
        #obr_34.1.3 - Given Name
        #obr_34.1.4 - Second And Further Given Names Or Initials Thereof
        #obr_34.1.5 - Suffix
        #obr_34.1.6 - Prefix
        #obr_34.1.7 - Degree
        #obr_34.1.8 - Source Table
        #obr_34.1.9 - Assigning Authority - Namespace Id
        #obr_34.1.10 - Assigning Authority- Universal Id
        #obr_34.1.11 - Assigning Authority - Universal Id Type
        #obr_34.2 - Start Date/Time
        #obr_34.2.1 - Time
        #obr_34.2.2 - Degree Of Precision
        #obr_34.3 - End Date/Time
        #obr_34.3.1 - Time
        #obr_34.3.2 - Degree Of Precision
        #obr_34.4 - Point Of Care
        #obr_34.5 - Room
        #obr_34.6 - Bed
        #obr_34.7 - Facility
        #obr_34.7.1 - Namespace Id
        #obr_34.7.2 - Universal Id
        #obr_34.7.3 - Universal Id Type
        #obr_34.8 - Location Status
        #obr_34.9 - Patient Location Type
        #obr_34.10 - Building
        #obr_34.11 - Floor
        #obr_35 - Transcriptionist
        #obr_35.1 - Name
        #obr_35.1.1 - Id Number
        #obr_35.1.2 - Family Name
        #obr_35.1.3 - Given Name
        #obr_35.1.4 - Second And Further Given Names Or Initials Thereof
        #obr_35.1.5 - Suffix
        #obr_35.1.6 - Prefix
        #obr_35.1.7 - Degree
        #obr_35.1.8 - Source Table
        #obr_35.1.9 - Assigning Authority - Namespace Id
        #obr_35.1.10 - Assigning Authority- Universal Id
        #obr_35.1.11 - Assigning Authority - Universal Id Type
        #obr_35.2 - Start Date/Time
        #obr_35.2.1 - Time
        #obr_35.2.2 - Degree Of Precision
        #obr_35.3 - End Date/Time
        #obr_35.3.1 - Time
        #obr_35.3.2 - Degree Of Precision
        #obr_35.4 - Point Of Care
        #obr_35.5 - Room
        #obr_35.6 - Bed
        #obr_35.7 - Facility
        #obr_35.7.1 - Namespace Id
        #obr_35.7.2 - Universal Id
        #obr_35.7.3 - Universal Id Type
        #obr_35.8 - Location Status
        #obr_35.9 - Patient Location Type
        #obr_35.10 - Building
        #obr_35.11 - Floor
        #obr_36 - Scheduled Date/Time
        #obr_36.1 - Time
        #obr_36.2 - Degree Of Precision
        #obr_37 - Number of Sample Containers
        #obr_38 - Transport Logistics of Collected Sample
        #obr_38.1 - Identifier
        #obr_38.2 - Text
        #obr_38.3 - Name Of Coding System
        #obr_38.4 - Alternate Identifier
        #obr_38.5 - Alternate Text
        #obr_38.6 - Name Of Alternate Coding System
        #obr_39 - Collector's Comment
        #obr_39.1 - Identifier
        #obr_39.2 - Text
        #obr_39.3 - Name Of Coding System
        #obr_39.4 - Alternate Identifier
        #obr_39.5 - Alternate Text
        #obr_39.6 - Name Of Alternate Coding System
        #obr_40 - Transport Arrangement Responsibility
        #obr_40.1 - Identifier
        #obr_40.2 - Text
        #obr_40.3 - Name Of Coding System
        #obr_40.4 - Alternate Identifier
        #obr_40.5 - Alternate Text
        #obr_40.6 - Name Of Alternate Coding System
        #obr_41 - Transport Arranged
        #obr_42 - Escort Required
        #obr_43 - Planned Patient Transport Comment
        #obr_43.1 - Identifier
        #obr_43.2 - Text
        #obr_43.3 - Name Of Coding System
        #obr_43.4 - Alternate Identifier
        #obr_43.5 - Alternate Text
        #obr_43.6 - Name Of Alternate Coding System
        #obr_44 - Procedure Code
        #obr_44.1 - Identifier
        #obr_44.2 - Text
        #obr_44.3 - Name Of Coding System
        #obr_44.4 - Alternate Identifier
        #obr_44.5 - Alternate Text
        #obr_44.6 - Name Of Alternate Coding System
        #obr_45 - Procedure Code Modifier
        #obr_45.1 - Identifier
        #obr_45.2 - Text
        #obr_45.3 - Name Of Coding System
        #obr_45.4 - Alternate Identifier
        #obr_45.5 - Alternate Text
        #obr_45.6 - Name Of Alternate Coding System
        #obr_46 - Placer Supplemental Service Information
        #obr_46.1 - Identifier
        #obr_46.2 - Text
        #obr_46.3 - Name Of Coding System
        #obr_46.4 - Alternate Identifier
        #obr_46.5 - Alternate Text
        #obr_46.6 - Name Of Alternate Coding System
        #obr_47 - Filler Supplemental Service Information
        #obr_47.1 - Identifier
        #obr_47.2 - Text
        #obr_47.3 - Name Of Coding System
        #obr_47.4 - Alternate Identifier
        #obr_47.5 - Alternate Text
        #obr_47.6 - Name Of Alternate Coding System
        #obr_48 - Medically Necessary Duplicate Procedure Reason.
        #obr_48.1 - Identifier
        #obr_48.2 - Text
        #obr_48.3 - Name Of Coding System
        #obr_48.4 - Alternate Identifier
        #obr_48.5 - Alternate Text
        #obr_48.6 - Name Of Alternate Coding System
        #obr_48.7 - Coding System Version Id
        #obr_48.8 - Alternate Coding System Version Id
        #obr_48.9 - Original Text
        #obr_49 - Result Handling
        #obr_50 - Parent Universal Service Identifier
        #obr_50.1 - Identifier
        #obr_50.2 - Text
        #obr_50.3 - Name Of Coding System
        #obr_50.4 - Alternate Identifier
        #obr_50.5 - Alternate Text
        #obr_50.6 - Name Of Alternate Coding System
        #obr_50.7 - Coding System Version Id
        #obr_50.8 - Alternate Coding System Version Id
        #obr_50.9 - Original Text

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
