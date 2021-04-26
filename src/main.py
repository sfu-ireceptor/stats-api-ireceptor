#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Created on: Dec 2020
# Last modified on: April 25 2021
# Author: Laura G. Funderburk

"""
Validating Stats API schema against JSON response
"""
import curlairripa
import pandas as pd
import airr
import argparse
import time
import yaml
import json
import sys
from xlrd import open_workbook, XLRDError


class ApiStats:

    def __init__(self, yaml_f, json_resp, facet_ct):
        self.yaml_f = yaml_f
        self.json_resp = json_resp
        self.facet_ct = facet_ct

    def load_yaml_schema(self):
        """


        Returns
        -------
        data_yaml : YAML file content
            contains Stats API schema in YAML format.

        """
        # Read schema
        # Assumes YAML format - modify to include JSON option
        try:
            with open(self.yaml_f, 'r') as f:

                data_yaml = yaml.load(f, Loader=yaml.FullLoader)

            f.close()

            print("Success!")

            return data_yaml

        except:
            print("Could not read YAML file")

    def get_total_count(self):
        """

        Returns
        dataframe
            json response of query formatter as a dataframe containing total
            stats for a given stats api query

        """
        try:
            all_df = []
            data_json = self.json_resp

            ids_ = pd.json_normalize(data_json['Result'])

            for entry in range(ids_.shape[0]):
                df = pd.json_normalize(data_json['Result'][entry]['statistics'])

                df['repertoire_id'] = ids_['repertoires.repertoire_id'].values[entry]
                df['sample_processing_id'] = ids_['repertoires.sample_processing_id'].values[entry]
                df['data_processing_id'] = ids_['repertoires.data_processing_id'].values[entry]

                all_df.append(df)

            return pd.concat(all_df)

        except:
            print("Could not find entries in json response")
            return pd.DataFrame({})

    def get_sum_count(self, original_count_df):
        """


        Parameters
        ----------
        original_count_df : dataframe
            object containing output resulting from get_total_count.

        Returns
        -------
        total_count_df : dataframe
            object containing output resulting from get_total_count plus
            additional columns SumOfCounts(StatsAPI) and ResultSum.
            The first accounts for the sum of all counts associated with a
            statistics, the latter verifies sum of count is equal to Total

        """

        try:

            total_count_df = original_count_df

            data_json = self.json_resp

            for i in range(total_count_df.shape[0]):
                reported_total = total_count_df.iloc[i, :]['total']
                sub_data = pd.json_normalize(data_json['Result'][0]['statistics'][i])
                # print(sub_data.columns)
                if 'data' not in sub_data.columns:
                    total_count_df.loc[i, 'SumOfCounts(StatsAPI)'] = 0
                    total_count_df.loc[i, 'ResultSum'] = 0
                    continue
                else:
                    sub_data = pd.json_normalize(data_json['Result'][0]['statistics'][i]['data'])

                    if "count" in sub_data.columns:
                        sum_count = sub_data['count'].sum()
                        total_count_df.loc[i, 'SumOfCounts(StatsAPI)'] = int(sum_count)

                        if reported_total == sum_count:
                            total_count_df.loc[i, 'ResultSum'] = True
                        else:
                            total_count_df.loc[i, 'ResultSum'] = False
                    else:
                        # print(total_count_df)
                        total_count_df.loc[i, 'SumOfCounts(StatsAPI)'] = -1
                        total_count_df.loc[i, 'ResultSum'] = -1

            return total_count_df
        except:
            print("Warning: could not perform sum of count query")


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


def validate_md_json_fields(base_url, query_files):
    """

    :param base_url: string with entry point where query will be performed (URL), facet count
    :param query_files: path to JSON file with input query
    :return: dataframe object with facet count results
    """
    # From class
    # Perform facet count
    query_url = base_url + "/airr/v1/" + "rearrangement"
    json_data = execute_query(query_url, query_files)

    if pd.json_normalize(json_data["Facet"]).empty:
        ir_seq_API = -1
        fac_count = pd.DataFrame({"repertoire_id": [0]})
    else:
        fac_count = pd.json_normalize(json_data["Facet"])
        ir_seq_API = str(fac_count['count'][0])

    result_suite = pd.DataFrame.from_dict({
        "RepertoireID(JSON)": [fac_count['repertoire_id'][0]],
        "FacetCountAPI": [ir_seq_API]})

    return result_suite


def read_file(path_to_file):
    """
    :param path_to_file:  (str) path to JSON file
    :return: JSON object imported into Python environment
    """
    try:
        with open(path_to_file, 'r') as f2:

            data_json = json.load(f2)

        f2.close()

        return data_json
    except:
        print("WARNING: check entry points and urls are valid. \nCould not parse json response")


def validate_headers(item_to_val):
    """
        This function uses AIRR's schema header validator function (see library import comment on version)

    :param item_to_val: either 'schemas' or 'responses' are valid options
    :return: None
    """

    iterate_over = airr.schema.Schema("components").definition[item_to_val].keys()

    for item in iterate_over:
        print(item, airr.schema.Schema("components").validate_header(item))


def validate_rows(item_to_val):
    """
    This function uses AIRR's schema row validator function (see library import comment on version)

    :param item_to_val: (str): either 'schemas' or 'responses' are valid options
    :return: None
    """

    rows = airr.schema.Schema("components").definition[item_to_val].keys()

    for row in rows:
        print(row, airr.schema.Schema("components").validate_row(
            airr.schema.Schema("components").definition[item_to_val][row]))


def validate_objects(item_to_val):
    """
    This function uses AIRR's schema object validator function (see library import comment on version)

    :param item_to_val: (str): either 'schemas' or 'responses' are valid options
    :return: None
    """
    objects = airr.schema.Schema("components").definition[item_to_val].keys()

    for obj in objects:
        print("Object", obj)
        print("Result", airr.schema.Schema("components").validate_object(
            airr.schema.Schema("components").definition[item_to_val][obj]))
        print("---")


def select_validator(validator_choice):
    """
    :param validator_choice: (string) One of 'None','headers','row','objects'
    :return: None
    """
    try:

        # Perform AIRR schema validation
        # Function picker
        testing_vars = ["schemas", "responses"]
        validation_selector = {"headers": validate_headers,
                               "rows": validate_rows,
                               "objects": validate_objects}

        # Perform test
        for testing_var in testing_vars:
            for item in validator_choice.split(","):
                if item == "None":
                    continue
                print()
                print("Performing validation at the", item, "level for", testing_var)
                print()
                validation_selector[item](testing_var)

    except:
        print("Error: verify validation is any combination of the following: 'rows','headers','objects'")
        print("Error: verify you are validating a stats API schema")


def execute_query(query_url, query_files):
    """

    :param query_url: string, entry point on which we perform query (URL)
    :param query_files: JSON file with query input parameters
    :return: parsed_query: (JSON object with response)
    """
    # Query parameters
    expect_pass = True
    verbose = False
    force = True

    # Ensure our HTTP set up has been done.
    curlairripa.initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = curlairripa.getHeaderDict()

    # Test query is well built, then perform query
    try:

        # Process json file into JSON structure readable by Python
        query_dict = curlairripa.process_json_files(force, verbose, query_files)

        # Perform the query. Time it
        start_time = time.time()
        query_json = curlairripa.processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
        total_time = time.time() - start_time

        # Parse
        parsed_query = json.loads(query_json)

        # Time
        print("ELAPSED DOWNLOAD TIME (in seconds): %s" % total_time)
        print("------------------------------------------------------")

        return parsed_query


    except:
        print("Read", query_url, " as entry point. Error found.")
        print("Error in URL - cannot complete query. Ensure the input provided points to an API")


def getArguments():
    """
    This function facilitates reading parameters

    """
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # URL associated with API
    parser.add_argument(
        "base_url",
        help="HTTP address associated with stats API"
    )

    # Entry point, options include:
    # /rearrangement/junction_length
    # /rearrangement/gene_usage
    # /rearrangement/count
    parser.add_argument(
        "entry_point",
        help="Options: string 'rearragement' or string 'repertoire'"
    )

    # JSON files containing query
    parser.add_argument(
        "stats_json_files",
        help="Enter full path to JSON queries"
    )

    # JSON files containing query
    parser.add_argument(
        "adc_json_files",
        help="Enter full path to ADC API JSON queries"
    )

    # Stats API YAML schema
    parser.add_argument(
        "yaml_file",
        help="Enter full path to YAML schema"
    )

    # Stats API YAML schema
    parser.add_argument(
        "validator_arr",
        help="Comma-separated list of words, each of which is an element from the list 'headers', 'rows', 'objects'"
    )

    # Directory where results will be saved
    parser.add_argument(
        "details_dir",
        help="Enter full path to JSON queries"
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


def stats_vs_facet_counts(stats_url, facet_url, repertoire_id, relative_path_stats, relative_path_facet):
    """

    :param stats_url: URL entry point for stats API
    :param facet_url: URL entry point for facet count
    :param repertoire_id: ID uniquely identifying repertoire
    :param relative_path_stats: path to JSON input files for Stats API queries
    :param relative_path_facet: path to JSON input files for facet count queries
    :return: [stats_response, facet_ct]
    """
    try:
        print("Perform Stats API query")
        # Form JSON input file
        stats_json_files = relative_path_stats + str(repertoire_id) + ".json"
        # Perform stats api query
        stats_response = execute_query(stats_url, stats_json_files)

        # Facet count query
        print("Facet count query")
        # Form JSON input file
        facet_query = relative_path_facet + str(repertoire_id) + ".json"
        # Sanity check facet count
        facet_ct = validate_md_json_fields(facet_url, facet_query)

        return [stats_response, facet_ct]

    except:
        print("Could not perform query for stats_vs_facet_counts()")
        print("Repertoire ID:", repertoire_id)
        print("Entry point facet:", facet_url)
        print("Facet JSON input", relative_path_facet)
        print("Entry point stats:", stats_url)
        print("Stats JSON input", relative_path_stats)


def generate_sum_count_total_test(details_dir,sum_count_total,stats_name):
    """
    :param details_dir: path to directory where results should be stored
    :param sum_count_total: list of dataframes with results of sum of count vs reported total
    :param stats_name: string, one of rearrangement_count, gene_usage, junction_length
    :return: None
    """
    try:
        print("Generating sum of count vs total results")
        final_sum_total_results = pd.concat(sum_count_total)
        final_sum_total_results.to_csv(details_dir + "COVID19-3" + "_" + stats_name + "_" + "SumCountTotalStat.csv")
    except:
        print("Received directory", details_dir)
        print("Received sum count total data structure", type(sum_count_total), " expected list of dataframes")
        print("Received", stats_name, " expected string")

def generate_results_file(details_dir, result_df,stats_name):
    """
    :param details_dir: path to directory where results should be stored
    :param result_df: list of dataframes with results of facet count vs reported total
    :param stats_name: string, one of rearrangement_count, gene_usage, junction_length
    :return: None
    """
    try:
        print("Generating facet count vs total results")
        final_results = pd.concat(result_df)
        final_results.to_csv(details_dir + "COVID19-3" + "_" + stats_name + "_" + "FinalCount.csv")
    except:
        print("Received directory", details_dir)
        print("Received results df data structure", type(result_df), " expected list of dataframes")
        print("Received", stats_name, " expected string")


def main():
    """

    This function performs stats API count vs facet count vs sum of count vs total
    For iReceptor API and COVID19 API
    :return: None
    """
    pd.set_option('display.max_columns', 500)

    print("STATS API TEST \n")
    # Input reading
    # =============================================================================
    #     options = getArguments()
    #     base_url = options.base_url
    #     #base_url = 'https://stats-staging.ireceptor.org'
    #     entry_pt = options.entry_point
    #     stats_json_files = options.stats_json_files
    #     adc_json_files = options.adc_json_files
    #     yaml_file = options.yaml_file
    #     validator_arr = options.validator_arr
    #     details_dir = options.details_dir
    # =============================================================================
    base_url = "http://covid19-3.ireceptor.org"
    entry_pt = "rearrangement/gene_usage"
    validator_arr = "None"
    details_dir = "./JSON-Files/yaml-verification/output/covid19-3/test/"
    adc_json_files = "./JSON-Files/repertoire/nofilters.json"
    # Parameters
    relative_path_stats = "./JSON-Files/yaml-verification/stats_query/covid19-3/stats_repertoire_id_"
    relative_path_facet = "./JSON-Files/yaml-verification/facet_query/covid19-3/facet_repertoire_id_"

    # Select validation
    print("Validation of schema option", validator_arr)
    select_validator(validator_arr)

    # Form metadata facet counts ADC API query
    adc_api_query_url = base_url + "/airr/v1/" + "repertoire"
    print("ADC API no filters query")
    # Perform metadata ADC API query
    json_adc_api_resp = execute_query(adc_api_query_url, adc_json_files)

    # Generate list of repertoire id's
    no_rep = len(json_adc_api_resp['Repertoire'])
    rep_ids = [json_adc_api_resp['Repertoire'][i]['repertoire_id'] for i in range(no_rep)]

    # Initialize lists with results
    # Facet count vs reported total
    result_df = []
    # Sum of count vs reported total
    sum_count_total = []

    # Initialize query entry points
    # Facet count entry point
    facet_url = base_url
    # Stats API entry point
    stats_url = base_url + "/irplus/v1/stats/" + entry_pt

    # Begin iteration
    for item in rep_ids:
        print("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*")
        time.sleep(1)
        # Iterate
        repertoire_id = str(item)
        print(repertoire_id)

        # Perform stats and facet count
        [stats_response, facet_ct] = stats_vs_facet_counts(stats_url,
                                                           facet_url,
                                                           repertoire_id,
                                                           relative_path_stats,
                                                           relative_path_facet)

        # Get total counts
        stats_api_ct = ApiStats(0, stats_response, 0).get_total_count()
        if stats_api_ct.empty:
            print("No entries found under stats count")
            print("Exiting script")
            sys.exit(0)

        # Sanity check
        stats_name = entry_pt.split("/")[1]

        # Sum of count vs total
        print("Perform sum of count vs reported total in stats api")
        stats_api_sum_count = ApiStats(0, stats_response, 0).get_sum_count(stats_api_ct)
        sum_count_total.append(stats_api_sum_count)

        # annotation_fc_ct = annotation_ct.merge(facet_ct,on='RepertoireID(MD)')
        annotation_fc_ct = facet_ct
        if stats_name == 'count':
            stats_name = "rearrangement_count"
        elif stats_name == 'gene_usage':
            stats_name = stats_name
            continue
        else:
            stats_name = stats_name
        stats_of_interest = stats_api_ct[(stats_api_ct["statistic_name"] == stats_name) &
                                         (stats_api_ct['repertoire_id'] == repertoire_id)]
        annotation_fc_ct['StatsAPICount'] = stats_of_interest['total'].values

        # Compare values
        run_test = int(annotation_fc_ct['FacetCountAPI'].values[0]) == int(annotation_fc_ct['StatsAPICount'].values[0])
        annotation_fc_ct["Result"] = run_test
        print("TEST RESULT---->", run_test)
        print(annotation_fc_ct)

        result_df.append(annotation_fc_ct)

    # Generate CSV with results
    generate_sum_count_total_test(details_dir, sum_count_total, stats_name)
    generate_results_file(details_dir, result_df, stats_name)


if __name__ == "__main__":
    main()
