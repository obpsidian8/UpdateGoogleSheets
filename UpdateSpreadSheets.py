import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from gspread.exceptions import APIError


class SheetErrorHandler:
    def __init__(self):
        print(f"LOG INFO: Setting up \"Error handler\"")
        pass

    def _open_sheet(self):
        print(f"LOG INFO: Error handler method")
        return

    def get_records(self):
        print(f"LOG INFO: Error handler method: get_records")
        return []

    def add_unique_record(self):
        print(f"LOG INFO: Error handler method: add_unique_record")
        return

    def get_most_recent_record(self):
        print(f"LOG INFO: Error handler method: get_most_recent_record")
        return None

    def get_oldest_record(self):
        print(f"LOG INFO: Error handler method: get_oldest_record")
        return None

    def get_records_matching_filter(self):
        print(f"LOG INFO: Error handler method: get_records_matching_filter")
        return None

    def add_none_unique_record(self):
        print(f"LOG INFO: Error handler method: add_none_unique_record")
        return None

    def delete_record(self):
        print(f"LOG INFO: Error handler method:delete_record ")
        return None


class UpdateSheet:
    def __init__(self, sheet_name: str, full_cred_file_name=f'helper_functions/client_secret.json'):
        print(f"LOG INFO: Setting up UpdateSheet instance for {sheet_name}")
        self.sheet_name = sheet_name
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(full_cred_file_name, self.scope)
        self.client = gspread.authorize(self.creds)

        self.sheet = None
        self._open_sheet()

    def _open_sheet(self):
        print(f"LOG INFO: Trying to establish connection to {self.sheet_name}")
        error = True
        tries = 0
        while error and tries <= 30:
            try:
                tries = tries + 1
                self.sheet = self.client.open(self.sheet_name).sheet1
                print(f"LOG SUCCESS: Connection to google sheet established!\n")
                error = False
            except APIError as e:
                print(f"LOG ERROR: API Error on _open_sheet!. Details {e}")
                error = True
                if tries < 20:
                    print(f"\nLOG INFO: Waiting for 10 seconds before retrying for try # {tries}")
                    timer = 10
                    while timer > 0:
                        print(f"Waiting for Google Api: {timer}")
                        time.sleep(1)
                        timer -= 1
            except Exception as other_e:
                print(f"LOG Error: Other exception occurred: Details {other_e} ")

        if self.sheet_name is None:
            self.e = SheetErrorHandler()

    def get_records(self):
        if self.sheet is None:
            return self.e.get_records()

        error = True
        tries = 0
        r = None

        while error and tries <= 30:
            try:
                tries = tries + 1
                r = self.sheet.get_all_records()
                print(f"LOG SUCCESS: Found records in spreadsheet")
                error = False
            except APIError:
                print(f"LOG ERROR: API Error on get_records")
                error = True
                if tries < 20:
                    print(f"\nLOG INFO: Waiting for 10 seconds before retrying for try # {tries}")
                    timer = 10
                    while timer > 0:
                        print(f"Waiting for Google Api: {timer}")
                        time.sleep(1)
                        timer -= 1
            except Exception as other_e:
                print(f"LOG Error: Other exception occurred: Details {other_e} ")

        if r is None:
            print(f"LOG ERROR: could not get records after {tries} tries ")
            return r

        if len(r) < 1:
            self.sheet.update_cell(2, 1, "Null")
            r = self.sheet.get_all_records()
        return r

    def add_unique_record(self, record_to_be_added: list, unique_column_name: str):
        if self.sheet is None:
            return self.e.add_unique_record()

        record_exists = False

        # Get all records from spreadsheet (List of dictionaries)
        records = self.get_records()

        # Get headers by using the first entry in the list of dictionaries
        # This is the number of columns
        headers = list((records[0]).keys())

        # Check if record_to_be_added to be added has all the necessary columns
        if len(record_to_be_added) != len(headers):
            print(f"LOG ERROR: Mismatch between length of record to be added and spread sheet columns")
            if len(record_to_be_added) > len(headers):
                print(f'LOG ERROR: Current record has more columns than spreadsheet')
                return False
            else:
                print(f"LOG ERROR: Current record is missing values in some columns. Empty columns will be set to null")
                while len(record_to_be_added) < len(headers):
                    record_to_be_added.append("Null")

        # To get the index of the item we have to check in the current record_to_be_added to add, we check what the index of the unique column name is from the list of headers
        check_idx = headers.index(unique_column_name)

        # Therefore the value we have to check from the current record_to_be_added to add is:
        check_value = record_to_be_added[check_idx]

        # We now have to check all records to see if the key/ unique value is already present
        for r in records:
            if str(r[unique_column_name]).strip() == str(check_value).strip():
                record_exists = True
                break

        if not record_exists:
            num_rows = len(records)
            last_index = num_rows + 2

            error = True
            tries = 0

            while error and tries <= 30:
                try:
                    tries = tries + 1
                    self.sheet.insert_row(record_to_be_added, last_index)
                    print(f"LOG SUCCESS: New record with {unique_column_name} value {check_value} added")
                    error = False
                except APIError as e:
                    print(f"LOG ERROR: API limit! on add_unique_record : Details {e}")
                    if tries < 20:
                        print(f"\nLOG INFO: Waiting for 10 seconds before retrying for try # {tries}")
                        error = True
                        timer = 10
                        while timer > 0:
                            print(f"Waiting for Google Api: {timer}")
                            time.sleep(1)
                            timer -= 1
                except Exception as other_e:
                    print(f"LOG Error: Other exception occurred: Details {other_e} ")
        else:
            print(f"LOG INFO: Value of {unique_column_name} ({check_value}) for current record already exists")

    def get_most_recent_record(self):
        if self.sheet is None:
            return self.e.get_most_recent_record()

        records = self.get_records()
        if not records:
            return None
        most_recent_record = records[-1]
        print(f"LOG SUCCESS: Most recent record found: {most_recent_record}")
        return most_recent_record

    def get_oldest_record(self):
        if self.sheet is None:
            return self.e.get_oldest_record()

        records = self.get_records()
        if not records:
            return None
        oldest_record = records[0]
        print(f"LOG SUCCESS: Oldest record found: {oldest_record}")
        return oldest_record

    def get_records_matching_filter(self, filter: str):
        filter = str(filter)
        """
        Returns a list of dict objects in which any column contains the specified filter.

        :param filter:
        :return:List of dictionaries
        """
        if self.sheet is None:
            return self.e.get_records_matching_filter()

        all_records = self.get_records()
        match_list = []

        for rec in all_records:
            if filter in rec.values():
                match_list.append(rec)

        if len(match_list) < 1:
            print(f"LOG INFO: Filter term  {filter} not found in sheet (get_records_matching_filter)")

        print(f"LOG INFO: Found filter: {filter}. NUmber of records: {len(match_list)}")
        return match_list

    def add_none_unique_record(self, record_to_be_added: list):
        if self.sheet is None:
            return self.e.add_none_unique_record()

        records = self.get_records()
        num_rows = len(records)
        last_index = num_rows + 2

        error = True
        tries = 0

        while error and tries <= 30:
            try:
                tries = tries + 1
                self.sheet.insert_row(record_to_be_added, last_index)
                print(f"LOG INFO: New record {record_to_be_added} added")
                error = False
            except APIError as e:
                print(f"LOG ERROR: API limit! on add_none_unique_record : Details {e}")
                if tries < 20:
                    print(f"\nLOG INFO: Waiting for 10 seconds before retrying for try # {tries}")
                    error = True
                    timer = 10
                    while timer > 0:
                        print(f"Waiting for Google API: {timer}")
                        time.sleep(1)
                        timer -= 1
            except Exception as other_e:
                print(f"LOG Error: Other exception occurred: Details {other_e} ")

    def _get_records_object_matching_filter(self, filter: str):
        filter = str(filter)
        if self.sheet is None:
            return self.e.get_records_matching_filter()

        error = True
        tries = 0
        r_cell_list = None

        while error and tries <= 10:
            try:
                tries = tries + 1
                r_cell_list = self.sheet.findall(filter)
                error = False
            except APIError as e:
                print(f"LOG ERROR: API limit! on get_records_matching_filter: Details {e}")
                if tries < 10:
                    print(f"\nLOG INFO: Waiting for 100 seconds before retrying for try # {tries}")
                    error = True
                    timer = 100
                    while timer > 0:
                        print(f"Waiting for Google Api: {timer}")
                        time.sleep(1)
                        timer -= 1
            except Exception as other_e:
                print(f"LOG Error: Other exception occured: Details {other_e} ")

        if r_cell_list is None:
            print(f"LOG ERROR: Could not get records after {tries}")
            return r_cell_list

        if len(r_cell_list) < 1:
            print(f"LOG INFO: Filter term  {filter} not found in sheet (_get_records_object_matching_filter)")

        print(f"LOG INFO: Found filter: {filter} in  {r_cell_list}")
        return r_cell_list

    def update_record(self, filter: str, updated_rec: list):
        """
        Updates a rows matching the filter supplied. (The row much contain the filter supplied)
        :param filter: filter matching a cell from the record
        :return:
        """
        # Need to get list of headers to know how many columns are in sheet
        header_values_list = self.sheet.row_values(1)
        num_columns = len(header_values_list)

        filter = str(filter)
        list_records_to_update = self._get_records_object_matching_filter(filter)
        # Loop through list of objects and get the row information for each
        updated_rows_list = []
        for rec in list_records_to_update:
            row = rec.row
            if row in updated_rows_list:
                continue

            print(f"\nLOG INFO: Updating record at row #{row}")

            # We have the number of columns. We will use the number of columns to update each field in the record with each entry of the updated record
            for col in range(num_columns):
                print(f"\tUPDATING: field #{col + 1}")
                self.sheet.update_cell(row, col + 1, updated_rec[col])
            updated_rows_list.append(row)

    def delete_record(self, value: str):
        value = str(value)
        """
        Deletes a row (or rows) that contains a given value
        :param value:
        :return:
        """
        if self.sheet is None:
            return self.e.delete_record()

        record_exists = True

        # After each deletion, there is the need to recalculate the indexes of the results that match the search
        while record_exists:
            # Get records that have a given value
            recs_to_del = self._get_records_object_matching_filter(value)
            if len(recs_to_del) < 1:
                # This means all the results matching the search have been deleted.
                record_exists = False

            deleted = False
            for rec in recs_to_del:
                if deleted is True:
                    # An item has been deleted, therefore there is the need to recalculate the indexes of the resuls the match the search value
                    break

                row = rec.row

                error = True
                tries = 0
                while error and tries <= 30:
                    try:
                        tries = tries + 1
                        self.sheet.delete_row(index=row)
                        print(f"LOG INFO: Record at row {row} deleted")
                        error = False
                        deleted = True
                    except APIError as e:
                        print(f"LOG ERROR: API limit! on add_none_unique_record : Details {e}")
                        if tries < 20:
                            print(f"\nLOG INFO: Waiting for 10 seconds before retrying for try # {tries}")
                            error = True
                            timer = 10
                            while timer > 0:
                                print(f"Waiting for Google API: {timer}")
                                time.sleep(1)
                                timer -= 1
                    except Exception as other_e:
                        print(f"LOG Error: Other exception occurred: Details {other_e} ")
