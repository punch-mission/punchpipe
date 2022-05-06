import sys
sys.path.append('/Users/jhughes/Desktop/repos/punchpipe/scripts')
from scripts.credentials import db_cred
from datetime import datetime
import mysql.connector
import shutil


def main():
    # set up database connection
    mydb = mysql.connector.connect(
      host="localhost",
      database=db_cred.project_name,
      user=db_cred.user,
      password=db_cred.password
    )
    mycursor = mydb.cursor()

    # copy to create example file
    this_time = datetime.now()
    example_name = "/Users/jhughes/Desktop/repos/punchpipe/example_run_data/PUNCH_L0_example.fits"
    datetime_str = this_time.strftime("%Y%m%d%H%M%S")
    new_name = f"/Users/jhughes/Desktop/repos/punchpipe/example_run_data/PUNCH_L0_XXX_{datetime_str}_v1.fits"
    shutil.copy(example_name, new_name)

    # create query to insert finished file for processing
    sql = "INSERT INTO files (level, file_type, observatory, file_version, software_version, date_acquired, date_obs, date_end, polarization, state, processing_flow) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = ("0", "XX", "X", 1, 1, datetime_str, datetime_str, datetime_str, "X", "finished", 1)
    print(val)
    mycursor.execute(sql, val)
    mydb.commit()


if __name__ == "__main__":
    main()
