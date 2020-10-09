# UpdateGoogleSheets
I used the Google API to create a python library that can be used to update a google sheet. From the developers console in your Google account, the Google sheet  API has to be enabled . At the end of the set up process, you will be able to download a credentials file that you will save. This file will be used in establishing a connection to the Sheets and modifying them.

A quick Google search for "Google sheets and Pyhon" will give numerous blog on how to set up the sheets api and download the credentials file.

To use with this library, this file should be renamed to "client_secret.json" and saved in s sub directory called "helper_funtions" in the same directory your file is in.


Dependencies:
gspread, 
oauth2client
