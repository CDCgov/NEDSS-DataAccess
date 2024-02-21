import os
from botocore.exceptions import ClientError
from faker import Faker
import json
import boto3
import datetime
import requests
import aiohttp
import asyncio
import zipfile
import io

# The HL7 fields that are checked by the deduplication algorithm for patient are as follows:
#    1. MSH 4.2 (SendingApplication.UniversalID) <- how do you do this?
#    2. OBR 3.1 (FillerOrderNumber.entity identifier) <- 01D0641691?
#    3. OBR 4.1 (Universal service ID.Identifier) 
#    4. OBR 4.4 (Universal service ID.Alternate Identifier)
#    5. OBR 7 (Observation Date/Time) <- 2.3.1
#    6. SPM 17 (Specimen Collection Date/Time) <- 2.5.1
#    7. PID.2.1 - Id Number
#    8. PID.2.5 - Identifier Type Code
#    9. PID.4.4 - Assigning Authority
#    10. PID.5.1 - Family Name
#    11. PID.5.2 - Given Name
#    12. PID.7 - Date/Time of Birth
#    13. PID.8 - Administrative Sex

num_messages=0 #default size

store_in_s3="false" #if 'true', the generated files will be stored in S3 bucket.
call_di_service="false" #if 'true', di service will be called to ingest the hl7 message.

di_api_url=""
auth_client_id=""
auth_client_secret=""
auth_token=""

### For local testing uncomment the following lines. DI api endpoint and auth details are hard coded.
# di_api_url = "https://dataingestion.dts1.nbspreview.com/api/reports"
# auth_client_id='di-keycloak-client'
# auth_client_secret='fcidpwabdQzUrPqEWkSzuGNX6EV4BJ7H1123432'
# auth_token=""

auth_headers ={}

################    
# S3 bucket 
s3_client=None
s3bucket_name="" 

async def generate_unique_patient_messages(num_messages, output_folder):
    fake = Faker()
    # os.makedirs(output_folder, exist_ok=True)

    print ("call_di_service flag:",call_di_service)
    print ("store_in_s3 flag:",store_in_s3)

    # Create a zip file in-memory and store it in S3 bukcet.
    if store_in_s3=='true':
        global s3bucket_name
        s3bucket_name = "hl7-generator"
        global s3_client
        s3_client = boto3.client('s3')
        zip_buffer = io.BytesIO()
        zipf=zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED)
    
    global auth_headers
    auth_headers = {'msgType':'HL7','accept':'*/*','Content-Type':'text/plain','clientId':auth_client_id,
               'clientSecret':auth_client_secret,'Authorization': f'Bearer {auth_token}'} 
    
    async with aiohttp.ClientSession() as session:
        for _ in range(num_messages):
            id_number = fake.random_int(min=10000, max=99999)
            patient_id = fake.random_int(min=10000, max=99999)
            person_number = fake.random_int(min=10000, max=99999)
            family_name = fake.last_name() 
            given_name = fake.first_name()
            name1= fake.first_name()
            name1last= fake.last_name() 
            name2= fake.first_name()
            name2last =fake.last_name() 
            date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y%m%d')
            date = fake.date()
            administrative_sex = fake.random_element(elements=('M', 'F', 'Other'))
            ssn = fake.ssn()
            address = fake.address()
            state = fake.state()
            city = fake.city()
            zipcode = fake.zipcode()
            phone = fake.phone_number()
            assigning_authority = fake.company()
            assigning_authority_id = fake.random_int(min=10000, max=99999)
            sending_app_id = fake.random_int(min=10000, max=99999)
            filler_order_entity_id = fake.random_int(min=0, max=999)
            apt_no = fake.random_int(min= 100, max= 10000)
            alternate_identifier = fake.random_int(min= 999, max= 99999)
            phone_part1 = fake.random_int(min=100, max=999)
            phone_part2 = fake.random_int(min=100, max=999)
            phone_part3 = fake.random_int(min= 1000, max = 9999)
            fake_message = {
                "data":
                f"MSH|^~\&|LABCORP-CORP^OID^ISO|LABCORP^34D0655059^CLIA|ALDOH^OID^ISO|AL^OID^ISO|200604040100||ORU^R01^ORU_R01|20120509010020114_251.2|D|2.5.1|||NE|NE|USA||||V251_IG_LB_LABRPTPH_R1_INFORM_2010FEB^^2.16.840.1.114222.4.3.2.5.2.5^ISO\r"
                f"SFT|Mirth Corp.|2.0|Mirth Connect|789654||20110101\r"
                f"PID|1|{patient_id}^^^^SS|{person_number}^^^Baker-Robbins&94534&CLIA^PN||{family_name}^{given_name}^^^^^^^^^^||{date_of_birth}|{administrative_sex}|||0605 Lin Creek Apt. {apt_no} Davieshaven, RI 70327^^West Rebecca^Vermont^95855||({phone_part1}){phone_part2}-{phone_part3}||||||{ssn}\r"
                f"ORC|RE||20120601{filler_order_entity_id}^LABCORP^34D0655059^CLIA||||||||||||||||||COOSA VALLEY MEDICAL CENTER|315 WEST HICKORY ST.^SUITE 100^SYLACAUGA^AL^35150^USA^^^RICHLAND|^^^^^256^2495780^123|380 WEST HILL ST.^^SYLACAUGA^AL^35150^USA^^^RICHLAND\r"
                f"OBR|1||20120601{filler_order_entity_id}^LABCORP^34D0655059^CLIA|699-9^ORGANISM COUNT^LN^080186^CULTURE^L|||200603241655|200603241655||342384^JONES^SUSAN||||||46466^BRENTNALL^GERRY^LEE^SR^DR^MD|^^^^^256^2495780|||||200604040139|||F|||46214^MATHIS^GERRY^LEE^SR^DR^MD~44582^JONES^THOMAS^LEE^III^DR^MD~46111^MARTIN^JERRY^L^JR^DR^MD|||12365-4^TOTALLY CRAZY^I9|22582&JONES&TOM&L&JR&DR&MD|22582&MOORE&THOMAS&E&III&DR&MD|44&JONES&SAM&A&JR&MR&MT|82&JONES&THOMASINA&LEE ANN&II&MS&RA\r"
                f"OBX|1|CE|11475-1^MICROORGANISM IDENTIFIED^LN^080187^RSLT#1^L|1|L-1F701^HAEMOPHILUS INFLUENZAE^SNM^HAEMIN^HAEMOPHILUS INFLUENZAE^L|MG|NEGATIVE|H|||F||||34D0655059^LABCORP BIRMINGHAM^CLIA||||20060401||||Lab1^L^^^^CLIA&2.16.840.1.114222.4.3.2.5.2.100&ISO^^^^1234|1234 Cornell Park Dr^^Blue Ash^OH^45241|"
            }
            #json_message = json.dumps(fake_message, indent=2)
            # print(json_message)
            hl7_text_message = json.dumps(fake_message['data'], indent=2).replace('"', '').encode('UTF-8')
            #print(hl7_text_message)
            
            # Ingest the HL7 message by calling the DI service
            if call_di_service=='true':
                coro = ingest_hl7_into_diservice(session,hl7_text_message)
                await coro
            
            # Put the HL7 messages as text files in a zip file, and store it in the S3 bucket.
            if store_in_s3=='true':
                date_time_now=datetime.datetime.now().strftime("%Y%m%d%f")
                filename=family_name+given_name+"-"+date_time_now+".txt"
                zipf.writestr(filename, hl7_text_message)

            # Create a separate JSON file for each message
            # file_name = os.path.join(output_folder, f"{given_name}_{family_name}.json")
            # with open(file_name, 'w') as json_file:
            #     json_file.write(json_message)
                
        # Upload the created zip file to the S3 bucket.
        if store_in_s3=='true':
            zipf.close()
            zip_buffer.seek(0)
            try:
                zip_file_name='hl7testfiles_'+datetime.datetime.now().strftime("%Y%m%d%H%M%S")+".zip"
                s3_client.upload_fileobj(zip_buffer, s3bucket_name, zip_file_name)
            except ClientError as cex:
                print(f'Error uploading to s3: {cex} ')
                raise cex

async def ingest_hl7_into_diservice(session,hl7message):
    async with session.post(di_api_url, data=hl7message, headers=auth_headers,ssl=False,raise_for_status=custom_error_handler) as response:
        text = await response.text()
        print(f'Response from di service status: {response.status} ID: {text} ')

async def custom_error_handler(response):
    if response.status >=400:
        text = await response.text()
        raise RuntimeError(text)

def reset_inputparams():
    global num_messages
    num_messages=0
    global store_in_s3
    store_in_s3="" 
    global call_di_service
    call_di_service="" 
    global di_api_url
    di_api_url=""
    global auth_client_id
    auth_client_id=""
    global auth_client_secret
    auth_client_secret=""
    global auth_token
    auth_token=""

# AWS Lambda invokes the function lambda_handler.
def lambda_handler(event, context):
    
    #reset the input values on each request.
    reset_inputparams()

    if "queryStringParameters" in event:
        print(event["queryStringParameters"])
        queryParams=event["queryStringParameters"]

        global num_messages
        if "num_messages" in queryParams:
            num_messages=int(queryParams["num_messages"])

        if "store_in_s3" in queryParams:
            global store_in_s3
            store_in_s3=queryParams["store_in_s3"]

        if "call_di_service" in queryParams and queryParams["call_di_service"]=='true':
            global call_di_service
            call_di_service=queryParams["call_di_service"]
            print('input param call_di_service: ',call_di_service)
            global di_api_url
            di_api_url=queryParams["di_api_url"]
            global auth_client_id
            auth_client_id=queryParams["auth_client_id"]
            global auth_client_secret
            auth_client_secret=queryParams["auth_client_secret"]
            global auth_token
            auth_token=queryParams["auth_token"]
    ## For Async call.        
    loop = asyncio.get_event_loop()    
    result = loop.run_until_complete(generate_unique_patient_messages(num_messages,""))
    return {
        'statusCode': 200,
        'body': json.dumps('Process complete!')
    }
# Uncomment the following lines for the local development.
# if __name__ == "__main__":
#     asyncio.run(generate_unique_patient_messages(5, ""))