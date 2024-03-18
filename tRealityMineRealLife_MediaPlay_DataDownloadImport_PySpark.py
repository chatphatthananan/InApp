import pysftp
import time, os
from datetime import datetime, date, timedelta
import logging
from SGTAMProdTask import SGTAMProd
import shutil
import pyzipper
import codecs
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import lit, current_date

# Set up logging
log_filename = f"D:/SGTAM_DP/Working Project/InApp/log/tRealityMineRealLife_MediaPlay_Data_Download_Import_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO)
s = SGTAMProd()

# Get the date for yesterday and Format the date as a string in the desired format
yesterday = date.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

# Get the date for yesterday and Format the date as a string in the desired format
today = date.today()
today_str = today.strftime("%Y-%m-%d")

### For backdate the process only
# Enter the date(s) for the files you want the script to work on, default is yesterday date.
# dates_list = ["2023-04-28","2023-05-02","2023-05-03","2023-05-04","2023-05-05","2023-05-06","2023-05-07","2023-05-08", "2023-05-10", "2023-05-11", "2023-05-12", "2023-05-13"]
# dates_list = ["2023-09-27"] # This is for backdate import or import for specific ref file date(s). For example on 30 June, it will import refdate 29 june, so we will set 29 June as the date in the array/list variable.
#dates_list = ["2023-10-28"]
dates_list = [yesterday_str] # This should be the default for daily process

# Define SFTP connection details
host = 'xxx'
port = 22
username = 'xxx'
password = 'xxx'
remote_directory = '/TAM_OGS/RealityMine/'
local_directory = 'D:/SGTAM_DP/Working Project/InApp/data/'
local_archive_directory = 'D:/SGTAM_DP/Working Project/InApp/data/archived/'

# Define SFTP connection configuration
sftp_config = {
    'host': host,
    'port': port,
    'username': username,
    'password': password
}
# Disable host key checking (not recommended for production use)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None 

# A list that will store name(s) of usage file(s) that is/are empty
empty_useage_files = []

# a list to store information about number of lines in datafile
check_lines = []

try:
    #------------------------------------------------------------------------------------------------------------#
    # Download files 
    #------------------------------------------------------------------------------------------------------------#   
    try:
        # Connect to the SFTP server
        logging.info("Establishing connection to SFTP.")
        with pysftp.Connection(**sftp_config, cnopts=cnopts) as sftp:
            # Add a delay to allow the SFTP session to set up
            time.sleep(3)
            # Change to the remote directory
            sftp.chdir(remote_directory)
            logging.info("Connection established.")

            # Iterate over the list of dates and download the files
            logging.info("Starting download")
            for date in dates_list:
                # Convert the input date to a datetime object
                date_obj = datetime.strptime(date, "%Y-%m-%d")

                # Generate the start and end timestamps for the file name
                start_time = date_obj.strftime("%Y%m%dT000000Z")
                end_time = (date_obj + timedelta(days=1)).strftime("%Y%m%dT000000Z")

                file_name = f"GfK.GfKDigitalTrendsMedia-RealLife-{start_time}-{end_time}.zip"
                remote_path = f'{remote_directory}{file_name}'
                local_path = f'{local_directory}{file_name}'

                try:
                    # Download the file from the SFTP server
                    sftp.get(remote_path, local_path)
                    logging.info(f'Successfully downloaded {file_name}')
                    print(f'Successfully downloaded {file_name}')
                except Exception as e:
                    logging.info(f'Error downloading {file_name}: {str(e)}')
                    raise Exception(f'Error downloading {file_name}: {str(e)}')# Raise the exception to the outer exception clause
                
    except Exception as e:
        raise Exception(e)

    #------------------------------------------------------------------------------------------------------------#
    # Unzip files 
    #------------------------------------------------------------------------------------------------------------#   
    logging.info("Unzip the downloaded file(s)")
    try:
        for date in dates_list:
            # Convert the input date to a datetime object
            date_obj = datetime.strptime(date, "%Y-%m-%d")

            # Generate the start and end timestamps for the file name
            start_time = date_obj.strftime("%Y%m%dT000000Z")
            end_time = (date_obj + timedelta(days=1)).strftime("%Y%m%dT000000Z")

            file_name = f"GfK.GfKDigitalTrendsMedia-RealLife-{start_time}-{end_time}.zip"
            folder_name = f"GfK.GfKDigitalTrendsMedia-RealLife-{start_time}-{end_time}"
            zip_file_dir = f'{local_directory}{file_name}'

            file_to_extract = 'RealLifeMediaPlaySessions.txt'
            extracted_name = f'RealLifeMediaPlaySessions_{date}.txt'
            password = 'xxx'

            logging.info("Extracting the zip file(s)")
            print("Extracting the zip file(s)")
            with pyzipper.AESZipFile(zip_file_dir) as zf:
                zf.pwd = password.encode('utf-8')
                zf.extractall(path=local_directory)
                print(f'Extracted - {zip_file_dir}')
                logging.info(f'Extracted - {zip_file_dir}')
            logging.info(f"Extraction completed")
            
            #-------------------------------------------------------
            # Copy file, change to csv, delete away redundant files
            #-------------------------------------------------------
            shutil.move(f'{local_directory}{folder_name}/{file_to_extract}',f'{local_directory}{extracted_name}')
            logging.info(f"{extracted_name} is available now")
            print(f"{extracted_name} is available now")


            # Convert the file from UTF-16 to UTF-8
            with codecs.open(f'{local_directory}{extracted_name}', 'r', 'utf-16') as file:
                content = file.read()
                with codecs.open(f'{local_directory}{extracted_name}', 'w', 'utf-8') as outfile:
                    outfile.write(content)

            logging.info("Removing redundant files/folders")
            print("Removing redundant files/folders")
            shutil.rmtree(f'{local_directory}{folder_name}')
            logging.info(f'Removed - {local_directory}{folder_name}')
            print(f'Removed - {local_directory}{folder_name}')
            os.remove(f'{zip_file_dir}')
            logging.info(f'Removed - {zip_file_dir}\n')
            print(f'Removed - {zip_file_dir}\n')

    except Exception as e:
        logging.info(f"Exception occured in Unzip code part: {e}")
        print(f"Exception occured in Unzip code part: {e}")
        raise Exception(f"Exception occured in Unzip code part: {e}")   

    #------------------------------------------------------------------------------------------------------------#
    # Check if Media Play usage file is empty
    #------------------------------------------------------------------------------------------------------------# 
    try:
        logging.info("Check if the usage file is empty by checking filesize equals 0?")
        for date in dates_list:
            file_size = os.path.getsize(f'{local_directory}RealLifeMediaPlaySessions_{date}.txt')
            if file_size == 0:
                logging.info(f'EMPTY - {local_directory}RealLifeMediaPlaySessions_{date}.txt')
                print(f'EMPTY - {local_directory}RealLifeMediaPlaySessions_{date}.txt')
                empty_useage_files.append(f'RealLifeMediaPlaySessions_{date}.txt')
    except Exception as e:
        logging.info(f"Exception occured in empty file check part: {e}")
        print(f"Exception occured in empty file check part: {e}")
        raise Exception(f"Exception occured in empty file check part: {e}")
    
    #------------------------------------------------------------------------------------------------------------#
    # If there is at least one empty usage file, will send warning email, else continue with the other processes
    #------------------------------------------------------------------------------------------------------------#     
    try:
        #------------------------------------------------------------------------#
        # Send warning email if usage file is empty
        #------------------------------------------------------------------------#
        if len(empty_useage_files) > 0:
            logging.info(f"There are empty usage files: {empty_useage_files}")
            print(f"There are empty usage files: {empty_useage_files}")
            print("Sending error email.")
            logging.info("Sending error email.")
            email_body = f"<p>No data import as there are usage file(s) that is/are empty.</p><p>Date in the filename is ref_date - 1, date in the email title is email date.</p><p>File dates to be imported:</p><p>{dates_list}</p><p>File dates that have empty usage files:</p><p>{empty_useage_files}</p><p>You may want to check with RealityMine if this missing usage is correct. Back date import can be done by manually assign the file date to dates_list variable in the script.</p><p>*This is an auto generated email, do not reply to this email.</p>"
            email_kwargs = {
                'sender':'xxx',
                'to':'xxx',
                'subject':f'[WARNING] InApp tRealityMineRealLifeMediaPlay Import - {today_str}',
                'body':email_body,
                'is_html':True
            }
            s.send_email(**email_kwargs)
            logging.info("Warning Email sent.")
            print("Warning Email sent.")
        
        #------------------------------------------------------------------------#
        # No empty usage file, proceed with data import
        #------------------------------------------------------------------------#
        else:
            logging.info(f"No empty usage file, proceeding with data import\n")
            print(f"No empty usage file, proceeding with data import\n")

            #------------------------------------------------------------------------------------------------#
            # Create dataframes, prepare for Data import using PySpark, Pandas will have issues occasionally
            #------------------------------------------------------------------------------------------------#
            for date in dates_list:
                
                # Set up file path for PySpark DF
                csv_file_path = f'{local_directory}RealLifeMediaPlaySessions_{date}.txt'
                
                # pre-define values for ref_date and import_date columns
                ref_date = date # for ref_date column
                today_date = datetime.now().strftime("%Y-%m-%d") # for import_date column, get today's date, as of current this is not used for import date column
               
                logging.info(f"Creating dataframe from RealLifeMediaPlaySessions_{date}.txt")
                print(f"Creating dataframe from RealLifeMediaPlaySessions_{date}.txt")
                
                # Create a SparkSession
                spark = SparkSession.builder \
                    .appName('Read Text File') \
                    .config("spark.driver.extraClassPath", "D:\\SGTAM_DP\\Working Project\\InApp\\mssql-jdbc-12.4.0.jre11.jar") \
                    .getOrCreate()
                
                # Define the schema / columns, datatype and if its nullable
                schema = StructType([
                    StructField("GroupName", StringType(), True),
                    StructField("PanelistId", StringType(), True),
                    StructField("ParentClientID", IntegerType(), True),
                    StructField("AppClientKey", StringType(), True),
                    StructField("OsName", StringType(), True),
                    StructField("OsVersion", IntegerType(), True),
                    StructField("OS", StringType(), True),
                    StructField("DeviceManufacturer", StringType(), True),
                    StructField("DeviceModel", StringType(), True),
                    StructField("DeviceType", StringType(), True),
                    StructField("MediaPlayer", StringType(), True),
                    StructField("MediaId", StringType(), True),
                    StructField("Advert", StringType(), True),
                    StructField("StreamType", StringType(), True),
                    StructField("Title", StringType(), True),
                    StructField("Series", StringType(), True),
                    StructField("EpisodeTitle", StringType(), True),
                    StructField("EpisodeNumber", IntegerType(), True),
                    StructField("Description", StringType(), True),
                    StructField("Category", StringType(), True),
                    StructField("Channel", StringType(), True),
                    StructField("VisitTimeUtc", TimestampType(), True),
                    StructField("VisitTimeLocal", TimestampType(), True),
                    StructField("VisitDuration", IntegerType(), True),
                    StructField("MediaDuration", IntegerType(), True),
                    StructField("ProcessedDate", TimestampType(), True)
                ])
                
                # Read the file using the defined schema
                # header = false , mean dont treat first line of the file as the headers
                df = spark.read.format("csv") \
                    .option("delimiter", "\t") \
                    .option("header", "false") \
                    .schema(schema) \
                    .load(csv_file_path)
                
                logging.info(f"Dataframe was created from RealLifeMediaPlaySessions_{date}.txt")
                print(f"Dataframe was created from RealLifeMediaPlaySessions_{date}.txt")
                
                # Add columns "ref_date" and "import_date" with default values to the DataFrame
                df = df.withColumn("ref_date", lit(ref_date).cast(DateType())) \
                    .withColumn("import_date", current_date())
                
                logging.info(f"Created and added ref_date and import_date columns to dataframe")
                print(f"Created and added ref_date and import_date columns to dataframe")
                
                # Reorder the columns to have the new columns at the first and second positions
                column_order = ["ref_date", "import_date"] + df.columns[:-2]
                df = df.select(*column_order)
                
                logging.info(f"Moved ref_date and import_date columns to the start of dataframe")
                print(f"Moved ref_date and import_date columns to the start of dataframe")
                
                
                # write dataframe to SQL table
                logging.info(f"Importing dataframe into tRealityMineRealLifeMediaPlay")
                print(f"Importing dataframe into tRealityMineRealLifeMediaPlay")
                df.write \
                    .format("jdbc") \
                    .option("url", "jdbc:sqlserver:xxx:1433;databaseName=xxx;trustServerCertificate=true") \
                    .option("dbtable", "tRealityMineRealLifeMediaPlay") \
                    .option("user", "xxx") \
                    .option("password", "xxx") \
                    .mode("append") \
                    .save()
                
                logging.info(f"Imported dataframe into tRealityMineRealLifeMediaPlay successfully for ref_date {date}")
                print(f"Imported dataframe into tRealityMineRealLifeMediaPlay successfully for ref_date {date}")
                
                # check how many records in dataframe
                row_count = df.count()
                print(f'dataframe has {row_count} rows, please cross check with the data file')
                logging.info(f'dataframe has {row_count} rows, please cross check with the data file')

                #----------------------------------------------------------------------------------------------------------------------

                # This part to check how many lines in the raw data file to compare with number of rows in dataframe
                with open(csv_file_path, 'r', encoding='utf-8') as file:
                    lines = file.readlines()
                    non_empty_lines = [line for line in lines if line.strip() != ""]
                    line_count = len(non_empty_lines)
                    print(f'Raw file has {line_count} rows.')
                    logging.info(f'Raw file has {line_count} rows.')

                    # append the check information into dictionary and this will be print out in email body
                    current_group = {
                        'ref_date':date,
                        'count df': row_count,
                        'count raw file':line_count
                    }
                    # append to this list for each iteration
                    check_lines.append(current_group)

                #----------------------------------------------------------------------------------------------------------------------

            #------------------------------------------------------------------------#
            # Archive the usage files that have been imported
            #------------------------------------------------------------------------#
            logging.info(f"Archiving imported usage files.")
            print(f"Archiving imported usage files.")
            try:
                for date in dates_list:
                    extracted_name = f'RealLifeMediaPlaySessions_{date}.txt'
                    shutil.move(f'{local_directory}{extracted_name}',f'{local_archive_directory}{extracted_name}')
                    logging.info(f"Archived - {extracted_name}")
                    print(f"Archived - {extracted_name}")    
            except Exception as e:
                logging.info(f"Exception occured at archiving usage files stage: {e}")
                raise Exception(f"Exception occured at archiving usage files stage: {e}")

            #------------------------------------------------------------------------#
            # Send email after sucessfully data import
            #------------------------------------------------------------------------#
            print("Process ok.")
            logging.info("Process ok.")
            print("Sending successful email.")
            logging.info("Sending successful email.")
            email_body = f"<p>Data import into tRealityMineRealLifeMediaPlay completed successfully.</p><p>Date in the filename is ref_date - 1, date in the email title is email date.</p><p>Imported file date:</p><p>{dates_list}</p><p>Please check if number of records imported are same as the ones in raw file</p><p>{check_lines}</p><p>*This is an auto generated email, do not reply to this email.</p>"
            email_kwargs = {
                'sender':''xxx',
                'to':'xxx',
                'subject':f'[OK] InApp tRealityMineRealLifeMediaPlay Import - {today_str}',
                'body':email_body,
                'is_html':True
            }
            s.send_email(**email_kwargs)
            logging.info("Successful email sent.")
            print("Successful email sent.")

    except Exception as e:
        logging.info(f"Exception occured in send warning email section part: {e}")
        print(f"Exception occured in send warning email section part: {e}")
        raise Exception(f"Exception occured in send warning email section part: {e}")
  
except Exception as e:
#------------------------------------------------------------------------#
# Send exception email
#
#------------------------------------------------------------------------#
    logging.info('Entered the exception clause.')
    print('Entered the exception clause.')
    logging.info("Exception has occured, sending out ERROR email.")
    print("Exception has occured, sending out ERROR email.")
    email_body = f"<p>Data import into tRealityMineRealLifeMediaPlay failed.</p><p>{e}</p><p>Please check log at {log_filename}</p><p>Date in the filename is ref_date - 1, date in the email title is email date.</p><p>File dates to be imported:</p><p>{dates_list}</p><p>*This is an auto generated email, do not reply to this email.</p>"
    email_kwargs = {
        'sender':'xxx',
        'to':'xxx',
        'subject':f'[ERROR] InApp tRealityMineRealLifeMediaPlay Import - {today_str}',
        'body':email_body,
        'is_html':True,
        'filename':log_filename
    }
    s.send_email(**email_kwargs)
    logging.info("Error email sent.")
    print("Error email sent.")

finally:
    logging.info('Entered finally clause, process ends here.')
    print('Entered finally clause, process ends here.') 