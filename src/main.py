# REPERTOIRE SANITY TESTING PYTHON SCRIPT
# AUTHOR: LAURA GUTIERREZ FUNDERBURK
# SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
# CREATED ON: December 5 2019
# LAST MODIFIED ON: April 21 2021

from curlairripa import *  # https://test.pypi.org/project/curlairripa/
import time  # time stamps
import pandas as pd
import argparse  # Input parameters from command line
import os
import sys
import airr
from xlrd import open_workbook, XLRDError
import subprocess
import tarfile
import math
import requests

pd.set_option('display.max_columns', 500)

class SanityCheck:
    def __init__(self, metadata_df, json_data, annotation_dir, repertoire_id):
        self.metadata_df = metadata_df
        self.json_data = json_data
        self.annotation_dir = annotation_dir
        self.repertoire_id = repertoire_id

    def test_book(self):

        """This function verifies whether it is possible to open a metadata EXCEL file.

        It returns True if yes, False otherwise"""

        filename = self.metadata_df

        try:
            open_workbook(filename)
            print("HEALTHY FILE: Proceed with tests\n")
        except XLRDError:
            print("CORRUPT FILE: Please verify master metadata file\n")
            print("INVALID INPUT\nInput is an EXCEL metadata file.")
            sys.exit()

    # Get appropriate metadata sheet
    def get_metadata_sheet(self):

        """This function extracts the 'metadata' sheet from an EXCEL metadata file """

        # Tabulate Excel file
        master_metadata_file = self.metadata_df
        table = pd.ExcelFile(master_metadata_file)  # ,encoding="utf8")
        # Identify sheet names in the file and store in array
        sheets = table.sheet_names
        # How many sheets does it have
        number_sheets = len(sheets)

        # Select metadata spreadsheet
        metadata_sheet = ""
        for i in range(number_sheets):
            # Check which one contains the word metadata in the title and hold on to it
            if "Metadata" == sheets[i] or "metadata" == sheets[i]:
                metadata_sheet = metadata_sheet + sheets[i]
                break

        # This is the sheet we want
        metadata = table.parse(metadata_sheet)

        return metadata

    def parse_metadata_sheet(self, master):
        """


        Parameters
        ----------
        master : dataframe
            file containing master metadata sheet ADC API.

        Returns
        -------
        sub_data_df : dataframe
            subset of the data containing data for a particular study.

        """
        # Get metadata and specific study
        master = master.loc[:, master.columns.notnull()]
        master = master.replace('\n', ' ', regex=True)
        # for master metadata only
        # grab the first row for the header
        new_header = master.iloc[1]
        # take the data less the header row
        master = master[2:]
        # set the header row as the df header
        master.columns = new_header
        # if "study_id" in master.columns and master['study_id'].isnull().sum()<1:
        if "study_id" in master.columns:
            master["study_id"] = master["study_id"].str.strip()
            master['study_id'] = master['study_id'].replace(" ", "", regex=True)
            # data_df = master.loc[master['study_id'] == study_id]
            sub_data_df = master

        else:

            sub_data_df = master

        return sub_data_df

    def ir_seq_count_imgt(self, data_df):
        """


        Parameters
        ----------
        data_df : TYPE
            DESCRIPTION.

        Returns
        -------
        result_suite : TYPE
            DESCRIPTION.

        """
        # From class
        annotation_dir = self.annotation_dir
        ir_rea = self.repertoire_id

        # subset and access data sources
        data_df = data_df[data_df['repertoire_id'] == int(ir_rea)]
        ir_file = data_df["data_processing_files"].to_list()[0].replace(" ", "")
        line_one = ir_file.split(",")
        files = os.listdir(annotation_dir)

        # Initialize structure

        number_lines = []
        sum_all = 0
        files_found = []
        files_notfound = []
        if "txz" not in ir_file:
            number_lines.append(0)
            sum_all = "NFMD"

        else:

            for item in line_one:
                if item in files:
                    files_found.append(item)
                    tf = tarfile.open(annotation_dir + item)
                    tf.extractall(annotation_dir + str(item.split(".")[0]) + "/")
                    stri = subprocess.check_output(
                        ['wc', '-l', annotation_dir + str(item.split(".")[0]) + "/" + "1_Summary.txt"])
                    hold_val = stri.decode().split(' ')
                    hold_val = [x for x in hold_val if x != '']
                    print(hold_val)
                    number_lines.append(hold_val[0])
                    sum_all = sum_all + int(hold_val[0]) - 1
                    # subprocess.check_output(['rm','-r',annotation_dir  + str(item.split(".")[0])+ "/"])
                else:
                    files_notfound.append(item)

        result_suite = pd.DataFrame.from_dict({"MetadataFileNames": [ir_file],
                                               "FilesFound": [files_found],
                                               "FilesNotFound": [files_notfound],
                                               "RepertoireID(MD)": [ir_rea],
                                               "NoLinesAnnotation": [sum_all]})

        return result_suite

    def ir_seq_count_igblast(self, data_df):
        """

        Parameters
        ----------
        data_df : TYPE
            DESCRIPTION.

        Returns
        -------
        result_suite : TYPE
            DESCRIPTION.

        """
        # From class
        annotation_dir = self.annotation_dir
        ir_rea = self.repertoire_id

        # subset and access data sources
        data_df = data_df[data_df['repertoire_id'] == int(ir_rea)]
        ir_file = data_df["data_processing_files"].to_list()[0].replace(" ", "")
        line_one = ir_file.split(",")
        files = os.listdir(annotation_dir)

        # Initialize structure

        number_lines = []
        sum_all = 0
        files_found = []
        files_notfound = []
        if "fmt" not in ir_file and "tsv" not in ir_file:
            number_lines.append(0)
            sum_all = "NFMD"
        else:
            for item in line_one:
                if item in files:
                    if "fmt19" in item:
                        files_found.append(item)
                        stri = subprocess.check_output(['wc', '-l', annotation_dir + str(item)])
                        hold_val = stri.decode().split(' ')
                        number_lines.append(hold_val[0])
                        sum_all = sum_all + int(hold_val[0]) - 1
                    elif "tsv" in item:
                        files_found.append(item)
                        stri = subprocess.check_output(['wc', '-l', annotation_dir + str(item)])
                        hold_val = stri.decode().split(' ')
                        number_lines.append(hold_val[0])
                        sum_all = sum_all + int(hold_val[0]) - 1
                    else:
                        continue
                else:
                    files_notfound.append(item)

        result_suite = pd.DataFrame.from_dict({"MetadataFileNames": [ir_file],
                                               "FilesFound": [files_found],
                                               "FilesNotFound": [files_notfound],
                                               "RepertoireID(MD)": [ir_rea],
                                               "NoLinesAnnotation": [sum_all]})

        return result_suite

    def validate_md_json_fields(self, data_df, base_url, query_files):
        """


        Parameters
        ----------
        data_df : TYPE
            DESCRIPTION.
        base_url : TYPE
            DESCRIPTION.
        query_files : TYPE
            DESCRIPTION.

        Returns
        -------
        result_suite : TYPE
            DESCRIPTION.

        """
        # From class
        ir_rea = self.repertoire_id

        # Perform facet count
        query_url = base_url + "/airr/v1/" + "rearrangement"
        query_json = execute_query(query_url, query_files)
        json_data = read_file(query_json)

        if pd.json_normalize(json_data["Facet"]).empty:
            ir_seq_API = -1
            fac_count = pd.DataFrame({"repertoire_id": [0]})
        else:
            fac_count = pd.json_normalize(json_data["Facet"])
            ir_seq_API = str(fac_count['count'][0])

            # Validate ir_curator_count is there
        if "ir_curator_count" in data_df.columns:
            message_mdf = ""
            ir_sec = data_df["ir_curator_count"].tolist()[0]
        else:
            message_mdf = "ir_curator_count not found in metadata"
            ir_sec = 0

        # Compare the numbers
        # Check MD count is NaN
        if math.isnan(ir_sec):
            ir_sec = "Null"
        else:
            ir_sec = int(ir_sec)

        result_suite = pd.DataFrame.from_dict({
            # "MessageMDF":[message_mdf],
            # "RepertoireID(MD)":[ir_rea],
            # "ir_curator":[ir_sec]
            "RepertoireID(JSON)": [fac_count['repertoire_id'][0]],
            "FacetCountAPI": [ir_seq_API]})

        return result_suite


# Section 1. Verify, read and parse files
# Test I can open file
def test_book(filename):
    """This function verifies whether it is possible to open a metadata EXCEL file.

    It returns True if yes, False otherwise"""
    try:
        open_workbook(filename)
    except XLRDError:
        return False
    else:
        return True


# Report whether file can be opened or not
def verify_non_corrupt_file(master_metadata_file):
    """This function verifies whether test_book returns True or False and prints a message to screen in either case"""

    try:
        if not test_book(master_metadata_file):
            print("CORRUPT FILE: Please verify master metadata file\n")
            sys.exit()

        else:
            print("HEALTHY FILE: Proceed with tests\n")
    except:
        print("INVALID INPUT\nInput is an EXCEL metadata file.")


# Get appropriate metadata sheet
def get_metadata_sheet(master_metadata_file):
    """This function extracts the 'metadata' sheet from an EXCEL metadata file """

    # Tabulate Excel file
    table = pd.ExcelFile(master_metadata_file)  # ,encoding="utf8")
    # Identify sheet names in the file and store in array
    sheets = table.sheet_names
    # How many sheets does it have
    number_sheets = len(sheets)

    # Select metadata spreadsheet
    metadata_sheet = ""
    for i in range(number_sheets):
        # Check which one contains the word metadata in the title and hold on to it
        if "Metadata" == sheets[i] or "metadata" == sheets[i]:
            metadata_sheet = metadata_sheet + sheets[i]
            break
            # Need to design test that catches when there is no metadata spreadsheet ; what if there are multiple
            # metadata sheets?

    # This is the sheet we want
    metadata = table.parse(metadata_sheet)

    return metadata


# Parse metadata sheet as pandas dataframe

def flatten_json(DATA):
    data_pro = pd.json_normalize(data=DATA['Repertoire'], record_path='data_processing')
    data_pro = rename_cols(data_pro, "data_processing")

    sample = pd.json_normalize(data=DATA['Repertoire'], record_path='sample')
    sample = rename_cols(sample, "sample")

    pcr_target = pd.json_normalize(DATA["Repertoire"], record_path=['sample', 'pcr_target'])
    pcr_target = rename_cols(pcr_target, "sample.0.pcr_target")

    subject = pd.json_normalize(data=DATA['Repertoire'], record_path=["subject", "diagnosis"])
    subject = rename_cols(subject, "subject.diagnosis")

    # print("================================================")
    repertoire = pd.json_normalize(data=DATA['Repertoire'])
    # print("================================================")

    # Optional
    concat_version = pd.concat([repertoire, data_pro, sample,
                                pcr_target, subject], 1).drop(["data_processing", "sample",
                                                               'sample.0.pcr_target'], 1)
    return concat_version


def get_dataframes_from_metadata(master_MD_sheet):
    """This function parses the metadata EXCEL sheet into a pandas dataframe

    EXCEL metadata sheets normally have 2 headers: internal-use headers and AIRR header

    This function creates a pandas dataframe using only the AIRR headers. This is the dataframe

    that the sanity checks will be performed on"""

    try:
        # Get the appropriate sheet from EXCEL metadata file
        data_dafr = get_metadata_sheet(master_MD_sheet)

        # grab the first row for the header
        new_header = data_dafr.iloc[1]
        # take the data less the header row
        data_dafr = data_dafr[2:]
        # set the header row as the df header
        data_dafr.columns = new_header

        return data_dafr
    except:
        print("INVALID INPUT\nInput is a single variable containing path and name to metadata spreadsheet.")


# Section 2. Sanity Checking
# Uniqueness and existence of field uniquely identifying each sample in metadata
def check_uniqueness_ir_rearrangement_nr(master_MD_dataframe, unique_field_id):
    """This function verifies that the unique field used to identify each sample exists and is unique in metadata"""

    try:
        print("Existence and uniquenes of " + str(unique_field_id) + " in metadata")

        # Check it exists
        if unique_field_id not in master_MD_dataframe.columns:
            print("WARNING: FIELD NAME DOES NOT EXIST TO UNIQUELY IDENTIFY SAMPLES IN THIS STUDY\n")
            print("Verify the column name exists and contains the correct information in your spreadsheet\n")
            sys.exit(0)

        else:
            # Check it is unique
            if not pd.Series(master_MD_dataframe[unique_field_id]).is_unique:
                print("FALSE: There are duplicate entries under " + str(unique_field_id) + " in master metadata\n")

            else:
                print("TRUE: All entries under  " + str(unique_field_id) + "  in master metadata are unique\n")
    except:

        print(
            'INVALID INPUT\nInput is a dataframe containing metadata and a field from metadata which uniquely \ '
            'identifies each sample.')


def ir_seq_count_imgt(data_df, repertoire_id, query_dict, query_url, header_dict, annotation_dir):
    connecting_field = 'repertoire_id'
    number_lines = []
    sum_all = 0
    files_found = []
    files_notfound = []

    ir_file = data_df["data_processing_files"].to_list()[0].replace(" ", "")
    line_one = ir_file.split(",")
    ir_rea = data_df[connecting_field].tolist()[0]
    ir_sec = data_df["ir_curator_count"].tolist()[0]
    files = os.listdir(annotation_dir)

    print(annotation_dir)

    if "txz" not in ir_file:
        number_lines.append(0)
        sum_all = "NFMD"

    else:

        for item in line_one:
            if item in files:
                files_found.append(item)
                tf = tarfile.open(annotation_dir + item)
                tf.extractall(annotation_dir + str(item.split(".")[0]) + "/")
                stri = subprocess.check_output(
                    ['wc', '-l', annotation_dir + str(item.split(".")[0]) + "/" + "1_Summary.txt"])
                hold_val = stri.decode().split(' ')
                number_lines.append(hold_val[0])
                sum_all = sum_all + int(hold_val[0]) - 1
                # subprocess.check_output(['rm','-r',annotation_dir  + str(item.split(".")[0])+ "/"])
            else:
                files_notfound.append(item)

        # Leave static for now
    expect_pass = True
    verbose = False
    force = True

    # Perform the query.
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
    json_data = json.loads(query_json)

    # Validate facet count is non-empty
    if pd.json_normalize(json_data["Facet"]).empty:
        ir_seq_API = "NINAPI"
        fac_count = pd.DataFrame({"repertoire_id": [0]})
    else:
        fac_count = pd.json_normalize(json_data["Facet"])
        ir_seq_API = str(fac_count['count'][0])

        # Validate ir_curator_count is there
    if "ir_curator_count" in data_df.columns:
        message_mdf = ""
        ir_sec = data_df["ir_curator_count"].tolist()[0]
    else:
        message_mdf = "ir_curator_count not found in metadata"
        ir_sec = 0

    # Compare the numbers
    # Check MD count is NaN
    if math.isnan(ir_sec):
        ir_sec = "Null"
    else:
        ir_sec = int(ir_sec)

    test_flag = {str(ir_seq_API), str(sum_all), str(ir_sec)}
    if len(test_flag) == 1:
        test_result = True
        print(ir_rea + " returned TRUE (test passed), see CSV for details")
    else:
        test_result = False
        print(ir_rea + " returned FALSE (test failed), see CSV for details")

    result_suite = pd.DataFrame.from_dict({"MetadataFileNames": [line_one],
                                           "FilesFound": [files_found],
                                           "FilesNotFound": [files_notfound],
                                           "MessageMDF": [message_mdf],
                                           "RepertoireID(MD)": [ir_rea],
                                           "RepertoireID(JSON)": [fac_count['repertoire_id'][0]],
                                           "FacetCountAPI": [ir_seq_API],
                                           "ir_curator": [ir_sec],
                                           "NoLinesAnnotation": [sum_all],
                                           "TestResult": [test_result]})

    return result_suite


def ir_seq_count_igblast(data_df, repertoire_id, query_dict, query_url, header_dict, annotation_dir):
    connecting_field = 'repertoire_id'
    number_lines = []
    sum_all = 0
    files_found = []
    files_notfound = []

    ir_file = data_df["data_processing_files"].to_list()[0].replace(" ", "")
    line_one = ir_file.split(",")
    ir_rea = data_df[connecting_field].tolist()[0]
    ir_sec = data_df["ir_curator_count"].tolist()[0]
    files = os.listdir(annotation_dir)

    if "fmt" not in ir_file and "tsv" not in ir_file:
        number_lines.append(0)
        sum_all = "NFMD"
    else:
        for item in line_one:
            if item in files:
                if "fmt19" in item:
                    files_found.append(item)
                    stri = subprocess.check_output(['wc', '-l', annotation_dir + str(item)])
                    hold_val = stri.decode().split(' ')
                    number_lines.append(hold_val[0])
                    sum_all = sum_all + int(hold_val[0]) - 1
                elif "tsv" in item:
                    files_found.append(item)
                    stri = subprocess.check_output(['wc', '-l', annotation_dir + str(item)])
                    hold_val = stri.decode().split(' ')
                    number_lines.append(hold_val[0])
                    sum_all = sum_all + int(hold_val[0]) - 1
                else:
                    continue
            else:
                files_notfound.append(item)

    # Leave static for now
    expect_pass = True
    verbose = False
    force = True

    # Perform the query.
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
    json_data = json.loads(query_json)

    # Validate facet query is non-empty
    if pd.json_normalize(json_data["Facet"]).empty:
        ir_seq_API = "NINAPI"
        fac_count = pd.DataFrame({"repertoire_id": [0]})
    else:
        fac_count = pd.json_normalize(json_data["Facet"])
        ir_seq_API = str(fac_count['count'][0])

        # Validate ir_curator_count exists
    if "ir_curator_count" in data_df.columns:
        message_mdf = ""
        ir_sec = data_df["ir_curator_count"].tolist()[0]
    else:
        message_mdf = "ir_curator_count not found in metadata"
        ir_sec = 0

    # Run test
    # Compare the numbers
    # Check MD count is NaN
    if math.isnan(ir_sec):
        ir_sec = "Null"
    else:
        ir_sec = int(ir_sec)

    test_flag = {str(ir_seq_API), str(sum_all), str(ir_sec)}
    if len(test_flag) == 1:
        test_result = True
        print(ir_rea + " returned TRUE (test passed), see CSV for details")
    else:
        test_result = False
        print(ir_rea + " returned FALSE (test failed), see CSV for details")

    result_suite = pd.DataFrame.from_dict({"MetadataFileNames": [line_one],
                                           "FilesFound": [files_found],
                                           "FilesNotFound": [files_notfound],
                                           "MessageMDF": [message_mdf],
                                           "RepertoireID(MD)": [ir_rea],
                                           "RepertoireID(JSON)": [fac_count['repertoire_id'][0]],
                                           "FacetCountAPI": [ir_seq_API],
                                           "ir_curator": [ir_sec],
                                           "NoLinesAnnotation": [sum_all],
                                           "TestResult": [test_result]})

    return result_suite


def ir_seq_count_mixcr(data_df, repertoire_id, query_dict, query_url, header_dict, annotation_dir):
    connecting_field = 'repertoire_id'
    number_lines = []
    sum_all = 0
    files_found = []
    files_notfound = []

    if type(data_df["data_processing_files"].tolist()[0]) == float:
        sys.exit()

    else:
        ir_file = data_df["data_processing_files"].tolist()[0].replace(" ", "")
        line_one = ir_file.split(",")
    ir_rea = data_df[connecting_field].tolist()[0]
    ir_sec = data_df["ir_curator_count"].tolist()[0]
    files = os.listdir(annotation_dir)

    if "txt" not in ir_file:
        number_lines.append(0)
        sum_all = "NFMD"

    else:

        for item in line_one:
            if item in files:

                files_found.append(item)
                stri = subprocess.check_output(['wc', '-l', annotation_dir + str(item)])
                hold_val = stri.decode().split(' ')
                number_lines.append(hold_val[0])
                sum_all = sum_all + int(hold_val[0]) - 1

            else:
                files_notfound.append(item)

        # Leave static for now
    expect_pass = True
    verbose = False
    force = True

    # Perform the query.
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)

    json_data = json.loads(query_json)
    # Validate query is non-empty

    if pd.json_normalize(json_data["Facet"]).empty:
        ir_seq_API = "NINAPI"
        fac_count = pd.DataFrame({"repertoire_id": [0]})
    else:
        fac_count = pd.json_normalize(json_data["Facet"])
        ir_seq_API = str(fac_count['count'][0])

        # Validate ir_curator_count exists
    if "ir_curator_count" in data_df.columns:
        message_mdf = ""
        ir_sec = data_df["ir_curator_count"].tolist()[0]
    else:
        message_mdf = "ir_curator_count not found in metadata"
        ir_sec = 0

        # Run test
    # Compare the numbers
    # Check MD count is NaN
    if math.isnan(ir_sec):
        ir_sec = "Null"
    else:
        ir_sec = int(ir_sec)

    test_flag = {str(ir_seq_API), str(sum_all), str(ir_sec)}
    if len(test_flag) == 1:
        test_result = True
        print(ir_rea + " returned TRUE (test passed), see CSV for details")
    else:
        test_result = False
        print(ir_rea + " returned FALSE (test failed), see CSV for details")

    result_suite = pd.DataFrame.from_dict({"MetadataFileNames": [line_one],
                                           "FilesFound": [files_found],
                                           "FilesNotFound": [files_notfound],
                                           "MessageMDF": [message_mdf],
                                           "RepertoireID(MD)": [ir_rea],
                                           "RepertoireID(JSON)": [fac_count['repertoire_id'][0]],
                                           "FacetCountAPI": [ir_seq_API],
                                           "ir_curator": [ir_sec],
                                           "NoLinesAnnotation": [sum_all],
                                           "TestResult": [test_result]})

    return result_suite


def rename_cols(flattened_sub_df, field_name):
    flattened_cols = flattened_sub_df.columns
    new_col_names = {item: str(field_name) + ".0." + str(item) for item in flattened_cols}
    flattened_sub_df = flattened_sub_df.rename(columns=new_col_names)

    return flattened_sub_df


def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Output Directory - where Performance test results will be stored
    parser.add_argument(
        "mapping_file",
        help="Indicate the full path to where the mapping file is found"
    )

    # Array with URL
    parser.add_argument(
        "base_url",
        help="String containing URL to API server  (e.g. https://airr-api2.ireceptor.org)"
    )
    # Entry point
    parser.add_argument(
        "entry_point",
        help="Options: string 'rearragement' or string 'repertoire'"
    )
    # Full path to directory with JSON file containing repertoire id queries associated to a given study
    parser.add_argument(
        "json_files",
        help="Enter full path to JSON query containing repertoire ID's for a given study - "
             "this must match the value given for study_id "
    )

    # Full path to metadata sheet (CSV or Excel format)
    parser.add_argument(
        "master_md",
        help="Full path to master metadata"
    )

    # Study ID (study_id)
    parser.add_argument(
        "study_id",
        help="Study ID (study_id) associated to this study"
    )

    # Full path to directory with JSON files containing facet count queries associated to each repertoire
    parser.add_argument(
        "facet_count",
        help="Enter full path to JSON queries containing facet count request for each repertoire"
    )

    # Full path to annotaton files
    parser.add_argument(
        "annotation_dir",
        help="Enter full path to where annotation files associated with study_id"
    )

    # Full path to directory where output logs will be stored
    parser.add_argument(
        "details_dir",
        help="Enter full path where you'd like to store content feedback in CSV format"
    )

    # Test type
    parser.add_argument(
        "Coverage",
        help="Sanity check levels: enter CC for content comparison, enter FC for facet count vs ir_curator "
             "count test, entNer AT for AIRR type test "
    )

    # Annotation tool
    parser.add_argument(
        "annotation_tool",
        help="Name of annotation tool used to process sequences. Choice between MiXCR, VQuest, IGBLAST"
    )

    # Verbosity flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode.")

    # Parse the command line arguements.
    options = parser.parse_args()
    return options


def main():
    print("DATA PROVENANCE TEST \n")
    # Input reading
    options = getArguments()
    mapping_file = options.mapping_file
    base_url = options.base_url
    entry_pt = options.entry_point
    query_files = options.json_files
    master_md = options.master_md
    study_id = options.study_id
    facet_ct = options.facet_count
    annotation_dir = options.annotation_dir
    details_dir = options.details_dir
    cover_test = options.Coverage
    annotation_tool = options.annotation_tool

    study_id = study_id.replace('/', '')

    connecting_field = 'repertoire_id'

    query_url = base_url + "/airr/v1/" + entry_pt

    # Leave static for now
    expect_pass = True
    verbose = True
    force = True

    # Ensure our HTTP set up has been done.
    initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = getHeaderDict()

    # Process json file into JSON structure readable by Python
    query_dict = process_json_files(force, verbose, query_files)

    # Perform the query. Time it
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)

    print("--------------------------------------------------------------------------------------------------------")


    filename = str(query_files.split("/")[-1].split(".")[0]) + "_" + str(study_id) + "__OUT.json"
    json_data = parse_query(query_json, str(details_dir) + str(query_files.split("/")[-1].split(".")[0]) + "_" + str(
        study_id) + "_")

    #     # Uncomment when AIRR test is ready to be used again
    if entry_pt == "repertoire":

        print("In repertoire entry point", entry_pt)

        try:
            airr.load_repertoire(str(details_dir) + filename, validate=True)
            print("Successful repertoire loading - AIRR test passed\n")
        except airr.ValidationError as err:
            print("ERROR: AIRR repertoire validation failed for file %s - %s" %
                  (filename, err))
            print("\n")
        print("--------------------------------------------------------------------------------------------------------")

    # Begin sanity checking
    print("########################################################################################################")
    print("---------------------------------------VERIFY FILES ARE HEALTHY-----------------------------------------\n")
    print("---------------------------------------------Metadata file----------------------------------------------\n")
    # GET METADATA
    try:
        if "xlsx" in master_md:
            verify_non_corrupt_file(master_md)
            master = get_dataframes_from_metadata(master_md)
        elif "csv" in master_md:
            master = pd.read_csv(master_md, encoding='utf-8')
            master = master.loc[:, ~master.columns.str.contains('^Unnamed')]

        elif "tsv" in master_md:

            master = pd.read_csv(master_md, encoding='utf8', sep="\t")
        elif "json" in master_md:

            florian_json = requests.get(master_md)
            florian_json = florian_json.json()
            master = flatten_json(florian_json)

    except:
        print("Warning: Provided wrong type file: cannot read metadata.")
        sys.exit(0)

    # Get metadata and specific study
    master = master.loc[:, master.columns.notnull()]
    master = master.replace('\n', ' ', regex=True)
    if "study_id" in master.columns and master['study_id'].isnull().sum() < 1:
        master["study_id"] = master["study_id"].str.strip()
        master['study_id'] = master['study_id'].replace(" ", "", regex=True)
        master['study_id'] = master['study_id'].str.replace('/', '')
        data_df = master.loc[master['study_id'] == study_id]
    else:
        data_df = master
    # data_df = data_df.replace('.00','', regex=True)

    input_unique_field_id = connecting_field
    # Check entries under unique identifier are  unique
    check_uniqueness_ir_rearrangement_nr(data_df, input_unique_field_id)

    if data_df.empty:
        print("EMPTY DATA FRAME: Cannot find specified study ID\n")
        print(data_df)
        sys.exit(0)

    no_rows = data_df.shape[0]

    # Mapping file
    map_csv = pd.read_csv(mapping_file, sep="\t", encoding="utf8", engine='python', error_bad_lines=False)
    ir_adc_fields = map_csv["ir_adc_api_response"].tolist()
    ir_cur_fields = map_csv["ir_curator"].tolist()
    ir_type_fileds = map_csv["airr_type"].tolist()
    rep_metadata_f = ir_cur_fields[0:89]
    rep_mappings_f = ir_adc_fields[0:89]
    rep_map_type = ir_type_fileds[0:89]

    # API response - wait until specs are done
    DATA = airr.load_repertoire(str(details_dir) + filename)

    print("================================================")

    concat_version = flatten_json(DATA)
    concat_version['study.study_id'] = concat_version['study.study_id'].replace(" ", "", regex=True)

    print("Cross comparison - field names\n")
    field_names_in_mapping_not_in_API = []
    field_names_in_mapping_not_in_MD = []
    in_both = []
    for f1, f2 in zip(rep_mappings_f, rep_metadata_f):
        if f1 not in concat_version.columns:
            field_names_in_mapping_not_in_API.append(f1)
        if f2 not in master.columns:
            field_names_in_mapping_not_in_MD.append(f2)
        if f1 in concat_version.columns and f2 in master.columns:
            in_both.append([f1, f2])

    # MAPPING FILE TESTING
    print("--------------------------------------------------------------------------------------------------------")
    print("Field names in mapping, ir_adc_api_response, not in API response\n")

    for item in field_names_in_mapping_not_in_API:
        if type(item) == float:
            continue
        else:
            print(item)

    print("--------------------------------------------------------------------------------------------------------")
    print("Field names in mapping, ir_curator, not in metadata fields\n")
    for item in field_names_in_mapping_not_in_MD:
        if type(item) == float:
            continue
        else:
            print(item)

    if connecting_field not in data_df.columns or "repertoire_id" not in concat_version.columns:
        print("Failure, need an ID to compare fields, usually " + str(
            connecting_field) + " in metadata file and repertoire_id in ADC API response. "
                                "If at least one of these is missing, the test cannot be completed.")

        sys.exit(0)
    else:
        # Get entries of interest in API response
        list_a = concat_version["repertoire_id"].to_list()
        int_list_a = [item for item in list_a]

        # Get corresponding entries in metadata
        sub_data = data_df[data_df[connecting_field].isin(int_list_a)]
        unique_items = sub_data[connecting_field].to_list()

    if len(unique_items) == 0:
        print(
            "WARNING: NON-MATCHING REPERTOIRE IDS - no id's match at ADC API and metadata level. "
            "Test results 'pass' as there is nothing to compare. Verify the repertoire ids in metadata are correct.")

    print("--------------------------------------------------------------------------------------------------------")

    # CONTENT TESTING
    if "CC" in cover_test:
        print("Content cross comparison\n")

        # Store information
        api_fields = []
        md_fields = []
        api_val = []
        md_val = []
        data_proc_id = []

        # Iterate over each rearrangement_number/repertoire_id
        for item in unique_items:

            # Get the row correspondong to the matching response in API
            rowAPI = concat_version[concat_version['repertoire_id'] == str(item)]

            rowMD = sub_data[sub_data[connecting_field] == item]

            # Content check
            for i in in_both:

                # Get row of interest
                md_entry = rowMD[i[1]].to_list()  # [0]
                API_entry = rowAPI[i[0]].to_list()  # [0]

                # Content is equal or types are equivalent
                try:
                    if md_entry == API_entry or API_entry[0] is None and type(md_entry[0]) == float or type(
                            API_entry[0]) == float and type(md_entry[0]) == float:
                        continue

                    elif type(md_entry[0]) != type(API_entry[0]) and str(md_entry[0]) == str(API_entry[0]):
                        continue
                    # Content mistmatch
                    else:
                        data_proc_id.append(item)
                        api_fields.append(i[0])
                        md_fields.append(i[1])
                        api_val.append(API_entry)
                        md_val.append(md_entry)

                except:
                    print("Cannot compare types")
                    print("-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*\n")
        # Report and store results
        content_results = pd.DataFrame({"DataProcessingID": data_proc_id,
                                        "API field": api_fields,
                                        "MD field": md_fields,
                                        "API value": api_val,
                                        "MD value": md_val})
        # Perfect results
        if content_results.empty:
            print("Could not find differring results between column content.")
        # Not so perfect results
        else:
            print("Some fields may require attention:")
            print("In ADC API: ", content_results["API field"].unique())
            print("In medatata: ", content_results["MD field"].unique())
            print("For details refer to csv")
            content_results.to_csv(
                details_dir + str(study_id) + "_reported_fields_" + str(pd.to_datetime('today')) + ".csv")

    print("--------------------------------------------------------------------------------------------------------")

    if "FC" in cover_test:
        print("Facet count vs ir_curator_count vs line count comparison\n")
        full_result_suite = []
        for item in unique_items:
            # print("---------------------------------------------------------------------------------------------------------------")
            # print("ITEM",item)
            rowAPI = concat_version[concat_version['repertoire_id'] == str(item)]

            rowMD = sub_data[sub_data[connecting_field] == item]

            time.sleep(1)
            # Process json file into JSON structure readable by Python
            query_dict = process_json_files(force, False, str(facet_ct) + str(study_id) + "/facet_repertoire_id_" + str(
                rowAPI['repertoire_id'].to_list()[0]) + ".json")

            ir_file = rowMD["data_processing_files"].tolist()[0]
            tool = rowMD["ir_rearrangement_tool"].to_list()[0]

            # Some entries may be empty - i.e. no files - skip but report
            if type(rowMD["data_processing_files"].to_list()[0]) == float:
                number_lines = []
                sum_all = 0
                print(
                    "FOUND ODD ENTRY: " + str(data_df["data_processing_files"].tolist()[0]) + "\nrepertoire_id " + str(
                        data_df["repertoire_id"].tolist()[
                            0]) + ". Writing 0 on this entry, but be careful to ensure this is correct.\n")
                number_lines.append(0)
                sum_all = sum_all + 0

                continue

            # Process each according to the tool used
            else:
                # CASE 1
                if tool == "IMGT high-Vquest" or annotation_tool.lower() == "vquest":

                    result_iter = ir_seq_count_imgt(rowMD, rowAPI['repertoire_id'].to_list()[0], query_dict,
                                                    base_url + "/airr/v1/rearrangement", header_dict, annotation_dir)
                    full_result_suite.append(result_iter)

                # CASE 2
                elif tool == "igblast" or annotation_tool.lower() == "igblast":
                    result_iter = ir_seq_count_igblast(rowMD, rowAPI['repertoire_id'].to_list()[0], query_dict,
                                                       base_url + "/airr/v1/rearrangement", header_dict, annotation_dir)
                    full_result_suite.append(result_iter)

                # CASE 3
                elif tool == "MiXCR" or annotation_tool.lower() == "mixcr":
                    result_iter = ir_seq_count_mixcr(rowMD, rowAPI['repertoire_id'].to_list()[0], query_dict,
                                                     base_url + "/airr/v1/rearrangement", header_dict, annotation_dir)
                    full_result_suite.append(result_iter)
                else:

                    print(
                        "WARNING: Could not find appropriate annotation tool: please specify one of 'MiXCR', 'IGBLAST' or 'VQUEST' in the annotation tool parameter")
        final_result = pd.concat(full_result_suite)
        final_result.to_csv(details_dir + str(study_id) + "_Facet_Count_curator_count_Annotation_count_" + str(
            pd.to_datetime('today')) + ".csv")
        print("For details on sequence count refer to " + str(
            study_id) + "_Facet_Count_curator_count_Annotation_count_" + str(pd.to_datetime('today')) + ".csv")

    print(
        "---------------------------------------------------------------------------------------------------------------")

    # AIRR TYPE - VERBOSE TEST
    if "AT" in cover_test:
        print("AIRR types vs ADC API types \n")

        x = float('nan')
        math.isnan(x)

        type_dict = {"boolean": bool, "integer": int, "number": float, "string": str, float('nan'): None}
        # Iterate over mapping files: mapping time : metadata file
        for cont, typ, met in zip(rep_mappings_f, rep_map_type, rep_metadata_f):
            # Skip if entry in mapping is empty
            if type(cont) == float:
                continue
            # Otherwise - iterate over each and compare types only when type match does not hold
            else:
                if isinstance(concat_version[cont].to_list()[0], list):
                    continue
                else:
                    types = []

                    for it in concat_version[cont].unique():
                        types.append(type(it))
                    u_type = set(types)

                    if next(iter(u_type)) == type_dict[typ]:
                        continue
                    else:
                        print("Field ADC API: ", cont, ".......................Field metadata:", met)
                        print("Unique metadata entries (content)", data_df[met].unique(),
                              "...............Unique ADC API entries (content)", concat_version[cont].unique())
                        print("ADC API content type", next(iter(u_type)))
                        print("AIRR type", type_dict[typ])
                print("\n")


if __name__ == "__main__":
    main()
