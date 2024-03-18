import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import os
from ftplib import FTP
from datetime import datetime
import shutil
import logging
from SGTAMProdTask import SGTAMProd


# Set up logging
log_filename = f"D:/SGTAM_DP/Working Project/InApp/log/tRealityMineAtlas_Data_Import{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO)

s = SGTAMProd()

try:
    #-----------------------------------------------------------------------------------------------------------------#
    # Download the latest file from Atlas FTP                                                                         #
    #-----------------------------------------------------------------------------------------------------------------#
    logging.info("Connect to Atlas FTP")
    # specify the FTP server details
    ftp_host = 'xxx'
    ftp_user = 'xxx'
    ftp_pass = 'xxx'

    # specify the remote and local directory paths
    remote_dir = '/OrgUnit075/Export/outgoing'
    local_dir = 'D:\\SGTAM_DP\\Working Project\\InApp\\data\\'

    # specify the desired filename for the downloaded file
    new_filename = 'AtlasFullyRealityMine.csv'

    # connect to the FTP server
    with FTP(ftp_host) as ftp:
        ftp.login(user=ftp_user, passwd=ftp_pass)
        logging.info("Connected to FTP.")
        ftp.cwd(remote_dir)

        # find the latest file with the name pattern 'AtlastReality'
        files = ftp.nlst()
        matching_files = [filename for filename in files if 'AtlasFullyRealityMine-' in filename]
        latest_file = max(matching_files, key=lambda filename: ftp.sendcmd('MDTM ' + filename))

        # download the latest file to the local directory
        local_file = os.path.join(local_dir, new_filename)
        with open(local_file, 'wb') as f:
            ftp.retrbinary('RETR ' + latest_file, f.write)
    logging.info(f'Downloaded latest file {latest_file} to {local_file} with new filename {new_filename}.')
    print(f'Downloaded latest file {latest_file} to {local_file} with new filename {new_filename}.')


    #-----------------------------------------------------------------------------------------------------------------#
    # Truncate the table and reinsert data into it again                                                              #
    #-----------------------------------------------------------------------------------------------------------------#
    logging.info("Connecting to SGTAMProdOIP")
    # Set up SQL connection
    server = 'xxx'
    database = 'xxx'
    username = 'xxx'
    password = 'xxx'
    driver = '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}")
    logging.info("Connected to SGTAMProdOIP")

    # create a SQLAlchemy engine using pyodbc
    engine = create_engine('mssql+pyodbc://', creator=lambda: cnxn)

    # Set up file paths and table name
    csv_file_path = "D:/SGTAM_DP/Working Project/InApp/data/AtlasFullyRealityMine.csv"
    table_name = 'tRealityMineAtlas'

    # Read in CSV file to a Pandas dataframe
    logging.info("Creating dataframe from exported file from Atlas")
    df = pd.read_csv(csv_file_path, delimiter='\t')
    df['SGP_MD2_EndTIme'] = pd.to_datetime(df['SGP_MD2_EndTIme'], format='%Y%m%d', errors='coerce')
    today_date = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df.insert(1, 'Data_Import_Datetime', today_date)
    logging.info("Added additional 'Data_Import_Datetime' column to the 2nd position in the dataframe")

    # Truncate existing SQL table
    logging.info("Truncate tRealityMineAtlas for the incoming data import")
    cursor = cnxn.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    cnxn.commit()

    logging.info("Import data into tRealityMineAtlas")
    # Insert dataframe into SQL table
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    
    # Import new records to SQL table
    logging.info("Import record(s) from tRealityMineAtlas to tRealityMineAtlasFixed")
    cursor = cnxn.cursor()
    cursor.execute(f"INSERT INTO [SGTAMProdOIP].[dbo].[tRealityMineAtlasFixed] SELECT * FROM [SGTAMProdOIP].[dbo].[tRealityMineAtlas] WHERE RespondentID NOT IN (SELECT RespondentID FROM [SGTAMProdOIP].[dbo].[tRealityMineAtlasFixed])")
    cnxn.commit()
    
    # Close SQL connection
    cnxn.close()
    logging.info("Data imported")

    #-----------------------------------------------------------------------------------------------------------------#
    # Archive the downloaded file                                                                                     #
    #-----------------------------------------------------------------------------------------------------------------#
    # source and destination directories
    dataFilesLocation = 'D:/SGTAM_DP/Working Project/InApp/data/'
    dataFilesLocationHistory = 'D:/SGTAM_DP/Working Project/InApp/data/archived/'
    # get the current datetime string
    now = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    files_to_be_moved = [
                        'AtlasFullyRealityMine.csv'
                        ]
    # iterate over all files in the source directory
    logging.info("Archive the downloaded file into archvied folder")
    for filename in os.listdir(dataFilesLocation):
        # check if the file is a text file
        if filename in files_to_be_moved:
            # construct the new filename with datetime string
            new_filename = f'{filename.split(".")[0]}_{now}.{filename.split(".")[1]}'
            # move the file to the destination directory with new filename
            shutil.move(os.path.join(dataFilesLocation, filename), os.path.join(dataFilesLocationHistory, new_filename))
except Exception as e:
    print("[ERROR] There is an exception.")
    print(e)
    logging.info("[ERROR] There is an exception.")
    logging.info(e)
    print("Sending error email.")
    logging.info("Sending error email.")
    email_body = f"<p>There is an error or exception in importing data into tRealityMineAtlas, please check D:/SGTAM_DP/Working Project/InApp/log/atlas_info_import{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt</p><p>{e}</p><p>This is an auto generated email, do not reply to this email.</p>"
    email_kwargs = {
        'sender':'xxx',
        'to':'xxx',
        'subject':'[ERROR] InApp tRealityMineAtlas Import',
        'body':email_body,
        'is_html':True
    }
    s.send_email(**email_kwargs)
    logging.info("Email sent.")
    # No error
else:
    logging.info("Task Completed!")
    print("Task Completed!")
    logging.info("Sending email.")
    print("Sending email.")
    email_body = f"<p>Atlas information has been imported into tRealityMineAtlas successfully.</p><p>This is an auto generated email, do not reply to this email.</p>"
    email_kwargs = {
        'sender':'xxx',
        'to':'xxx',
        'subject':'[OK] InApp tRealityMineAtlas Import',
        'body':email_body,
        'is_html':True
    }
    s.send_email(**email_kwargs)
    logging.info("Email sent.")
    print("Email sent.")
finally:
    logging.info("Finally clause completed.")
    print("Finally clause completed.")