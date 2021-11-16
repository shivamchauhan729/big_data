#!/usr/bin/python
# -*- coding: utf-8 -*-

#######################################Module Information########################################################
#  Module Name         : Initial/Incremental Decider for glue logic                                             #
#  Purpose             : Initial/Incremental Decider for glue logic 					                        #
#  Input Parameters    : Input json files                                                   					#
#  Output Value        : Will return #
#################################################################################################################

import sys
script_dir = '/home/hadoop/clean'
sys.path.insert(1, script_dir)

from configs.cleansed_run_constants import CleansedRunConstants
from python.utils.dataset_info import DatasetInfo


def get_run_mode(dataset_info_table, dataset_key, is_cross_account, cross_account_role_arn):
    resp = DatasetInfo().get_dataset(dataset_info_table, dataset_key, is_cross_account, cross_account_role_arn)
    print(resp["Item"]["run_mode"])


if __name__ == "__main__":
    dataset = sys.argv[1]
    dataset_info_table = CleansedRunConstants.DATASET_INFO_TABLE
    dataset_key = CleansedRunConstants.AUDIT_PREFIX + "_" + dataset
    is_cross_account = CleansedRunConstants.CROSS_ACCOUNT_FLAG
    cross_account_role_arn = CleansedRunConstants.CROSS_ACCOUNT_ROLE_ARN
    get_run_mode(dataset_info_table, dataset_key, is_cross_account, cross_account_role_arn)
