# Aviation
Aviation Data Processing

✈️ **Flight Data Processing with PySpark on Databricks**

This project demonstrates how to:

Mount external Azure Blob Storage in Databricks

Load flight data from CSV

Explore and transform data using PySpark

Perform data validation using assertions

Work with timestamps and calculate flight durations

📁 **Project Structure**
.
├── Flight_Data.ipynb 
├── README.md 

**Below are the steps involved**

**1. Mount Azure Blob Storage in Databricks**
     Mount an external storage to keep the files. This is not mandatory, we can keep this in DBFS directly. But here I am copying the         files to a mount point in Databricks. Mount point is nothing but an external storage.
   
**2. Upload the Flight_Data.csv file**
     Once the mount point is created, run the below script in your local terminal to copy the file from my local to the mount point.

           _databricks fs cp ~/Downloads/Flight_Data.csv dbfs:/mnt/adls/Flight_Data.csv_

     We can also do this by manually uploading the file to the storage path. (Upload directly in Azure in this case)
  
**3. Create a dataframe from the file and display the dataframe**
     Use read.format('csv') with header as true and inferSchema to get the schema from the file. I read the file from the mount point         created into a dataframe df_flight_data.

         _df_flight_data=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/adls/Flight_Data.csv")_
   
**4. Display the schema of the DataFrame**
     To display the schema, I am using the printSchema() function.
       
        _df_flight_data.printSchema()_
   
**5. Show the first 10 rows**
     Using limit(10) and display() function to show the first 10 records. We can also use df.show(10) or df.head(10).

         _df_flight_data.limit(10).display()_
         
**6. Count the total number of records**
     I am using count() to count the number of records in the dataframe and assigning it to a variable.

            _df_count=df_flight_data.count()
            display(df_count)_


**7. Filter out any records where passenger_count is less than or equal to 0**
     Using filter(condition) I am filtering records with passenger_count less than or equal to 0. There are no such records, so the           output doesn't show any values.
         _df_filter=df_flight_data.filter(df_flight_data['passenger_count'] <= 0)
          df_filter.display()_
    
**8. Add a new column, flight_duration_hours, that calculates the flight duration in hours.**
     To calculate the flight duration I am subracting the arrival_time and departure_time which will give the timestamp in seconds. So I convert this to hours by diving this by 60*60 (ie 3600) and round the values to 2 decimal places. Created a new dataframe df_with_duration with the new column duration_hours.

        _from pyspark.sql.functions import col, unix_timestamp, round
        # Convert string timestamps to timestamp type
        df_with_duration = df_flight_data.withColumn("duration_hours",
                                 round((unix_timestamp("arrival_time") - unix_timestamp("departure_time")) / 3600.0, 2))_
     
**9. Display df_with_duration**


**10. Adding Assertions on the dataframe**

      Checking if the origin_airport and destination_airport is the same. This should not be same. My assumption is we are looking for         only valid flights.
      Checking if the departure_time is greater than arrival_time. arrival time should be always greater than departure_time
      Checking if any of the column value is null. All the column looks like they are not nullable, so checking if there are any null          values.


