import pysftp
import sys, time, io, os
from datetime import datetime, date
import zipfile
import logging
from SGTAMProdTask import SGTAMProd
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import shutil

# Set up logging
log_filename = f"D:/SGTAM_DP/Working Project/InApp/log/tRealityMineCompliance_Data_Download_Import_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO)
s = SGTAMProd()

# Define the list of dates for which you want to download and import compliance reports
today = date.today().strftime("%Y_%m_%d")
dates_list = [today]
#dates_list = ['2023_09_28']
# dates_list = ['2023_05_11', '2023_05_12', '2023_05_13', '2023_05_14']

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
                file_name = f'GfK_Singapore_Pilot_Compliance_{date}.zip'
                remote_path = f'{remote_directory}/{file_name}'
                local_path = f'{local_directory}/{file_name}'

                try:
                    # Download the file from the SFTP server
                    sftp.get(remote_path, local_path)
                    logging.info(f'Successfully downloaded {file_name}')
                    print(f'Successfully downloaded {file_name}')
                except Exception as e:
                    logging.info(f'Error downloading {file_name}: {str(e)}')
                    raise Exception(f'Error downloading {file_name}: {str(e)}')# Raise the exception to the outer exception clause
    except Exception as e:
        raise e # Raise the exception to the outermost exception clause           
    #------------------------------------------------------------------------------------------------------------#
    # Unzip files 
    #------------------------------------------------------------------------------------------------------------#   
    logging.info("Unzip the downloaded file(s)")
    for date in dates_list:
        try:
            file_name = f'GfK_Singapore_Pilot_Compliance_{date}.zip'
            file_to_extract = 'GfK_Singapore_Pilot_Daily_Compliance.csv'
            extracted_name = f'GfK_Singapore_Pilot_Daily_Compliance_{date}.csv'
            password = 'xxx'
            with zipfile.ZipFile(f'{local_directory}{file_name}') as zf:
                for info in zf.infolist():
                    # Check if the current file in the archive matches the file we want to extract
                    if info.filename == file_to_extract:
                        # Extract the CSV file to memory and read its content
                        with zf.open(info, 'r', pwd=bytes(password, 'utf-8')) as f:
                            with io.TextIOWrapper(f) as csv_file:
                                csv_content = csv_file.read()

                        # Save the CSV content to the local directory without creating a folder
                        output_path = os.path.join(local_directory, os.path.basename(extracted_name))
                        with open(output_path, 'w') as output_file:
                            output_file.write(csv_content)
                    logging.info(f'Successfully extracted {file_name} to {output_path}')
                    print(f'Successfully extracted {file_name} to {output_path}')
                    break
                else:
                    # This block is executed
                    logging.info(f'CSV file "{file_to_extract}" not found in the archive.')
                    print(f'CSV file "{file_to_extract}" not found in the archive.')
        except Exception as e:
            logging.info("Exception occured in Unzip part, raising exception to the outer exception.")
            print("Exception occured in Unzip part, raising exception to the outer exception.")
            raise Exception(f'An error occurred: {e}')

    
    #------------------------------------------------------------------------------------------------------------#
    # Check if Compliance file is empty
    #------------------------------------------------------------------------------------------------------------# 
    try:
        logging.info("Check if the usage file is empty by checking filesize equals 0?")
        for date in dates_list:
            file_size = os.path.getsize(f'{local_directory}GfK_Singapore_Pilot_Daily_Compliance_{date}.csv')
            if file_size == 0:
                logging.info(f'EMPTY - {local_directory}GfK_Singapore_Pilot_Daily_Compliance_{date}.csv')
                print(f'EMPTY - {local_directory}GfK_Singapore_Pilot_Daily_Compliance_{date}.csv')
                empty_useage_files.append(f'GfK_Singapore_Pilot_Daily_Compliance_{date}.csv')
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
            print("Sending warning email.")
            logging.info("Sending error email.")
            email_body = f"<p>No data import as there are usage file(s) that is/are empty.</p><p>Date in the filename is ref_date - 1, date in the email title is email date.</p><p>File dates to be imported:</p><p>{dates_list}</p><p>File dates that have empty usage files:</p><p>{empty_useage_files}</p><p>You may want to check with RealityMine if this missing usage is correct. Back date import can be done by manually assign the file date to dates_list variable in the script.</p><p>*This is an auto generated email, do not reply to this email.</p>"
            email_kwargs = {
                'sender':'xxx',
                'to':'xxx',
                'subject':f'[WARNING] InApp tRealityMineRealLife Import - {today}',
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
    
    
            #------------------------------------------------------------------------------------------------------------#
            # Import data into tRealityMineCompliance
            #------------------------------------------------------------------------------------------------------------#   
            try:
                for date in dates_list:
                    logging.info("Connecting to SGTAMProdOIP")
                    # SQL Connection Infos
                    server = 'xxx'
                    database = 'xxx'
                    username = 'xxx'
                    password = 'xxx'
                    driver = '{ODBC Driver 17 for SQL Server}'
                    # Set up SQL connection
                    cnxn = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}")
                    logging.info("Connected to SGTAMProdOIP")

                    # create a SQLAlchemy engine using pyodbc
                    engine = create_engine('mssql+pyodbc://', creator=lambda: cnxn)

                    # Set up file paths and table name
                    csv_file_path = f'{local_directory}GfK_Singapore_Pilot_Daily_Compliance_{date}.csv'
                    table_name = 'tRealityMineCompliance'

                    # Read in CSV file to a Pandas dataframe
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', 1000)
                    ref_date = date.replace('_','-')
                    pd.set_option('display.max_rows', None)
                    today_date = datetime.now().strftime("%Y-%m-%d")

                    logging.info(f"Creating dataframe from GfK_Singapore_Pilot_Daily_Compliance_{date}.csv")
                    df = pd.read_csv(csv_file_path, delimiter=',')
                    logging.info(f"Going to add ref_date and import_date columns into the dataframe")
                    df.insert(0, 'ref_date', ref_date)
                    logging.info("Added additional 'ref_date' column to the 1st position in the dataframe")
                    df.insert(1, 'import_date', today_date)
                    logging.info("Added additional 'import_date' column to the 2nd position in the dataframe")        

                    # Insert dataframe into SQL table            
                    logging.info("Import data into tRealityMineCompliance")
                    df.to_sql(table_name, con=engine, if_exists='append', index=False)
                    logging.info("Data imported")

                    # Close SQL connection 
                    cnxn.close()
                    logging.info("SQL connection closed.")
            except Exception as e:
                raise Exception(f'An error occurred: {e}')

            #------------------------------------------------------------------------------------------------------------#
            # Archive data files
            #------------------------------------------------------------------------------------------------------------#  
            for date in dates_list:
                try:
                    os.remove(f"{local_directory}GfK_Singapore_Pilot_Compliance_{date}.zip")
                    logging.info(f"Removed - {local_directory}GfK_Singapore_Pilot_Compliance_{date}.zip")

                    shutil.move(f"{local_directory}GfK_Singapore_Pilot_Daily_Compliance_{date}.csv", f"{local_archive_directory}GfK_Singapore_Pilot_Daily_Compliance_{date}.csv")
                    logging.info(f"Archived - {local_directory}GfK_Singapore_Pilot_Daily_Compliance_{date}.csv")
                except Exception as e:
                    logging.info(f'An error occurred: {e}')
                    raise Exception(f'An error occurred: {e}')

        #------------------------------------------------------------------------#
        # Send email after sucessfully data import
        #------------------------------------------------------------------------#
        logging.info("Task Completed!")
        print("Task Completed!")
        logging.info("Sending email.")
        print("Sending email.")
        email_body = f"<p>RealityMine compliance information has been imported into tRealityMineCompliance successfully.</p><p>Imported file date:</p><p>{dates_list}</p><p>*This is an auto generated email, do not reply to this email.</p>"
        email_kwargs = {
            'sender':'xxx',
            'to':'xxx',
            'subject':'[OK] InApp tRealityMineCompliance Import',
            'body':email_body,
            'is_html':True
        }
        s.send_email(**email_kwargs)
        logging.info("Email sent.")
        print("Email sent.")

    except Exception as e:
        logging.info(f"{e}")
        print(f"{e}")
        raise Exception(f"{e}")

except Exception as e:
#------------------------------------------------------------------------#
# Send exception email
#------------------------------------------------------------------------#
    print(e)
    logging.info(e)
    print("Sending error email.")
    logging.info("Sending error email.")
    email_body = f"<p>There is an error or exception in the tRealityMineCompliance importing process, please check {log_filename}</p><p>{e}</p><p>File dates intended to be imported:</p><p>{dates_list}</p><p>*This is an auto generated email, do not reply to this email.</p>"
    email_kwargs = {
        'sender':'xxx',
        'to':'xxx',
        'subject':'[ERROR] InApp tRealityMineCompliance Import',
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