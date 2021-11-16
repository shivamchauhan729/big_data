#!/usr/bin/python
# -*- coding: utf-8 -*-

#######################################Module Information############################################
#  Module Name         : editable_space                                                             #
#  Purpose             : To provide editable code space for a developer who will use this framework #
#  Input Parameters    : Spark Session , input dataframe                                            #
#  Pre-requisites      : Spark Session to be created in parent module.                              #
#####################################################################################################


# Library and external modules declaration
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


class EditableSpace(object):
    """
        Class contains the method to add or edit code snippet.
    """

    def editable_snippet(self, spark, df):
        """
            This function is used to add or edit any business logic with respect to specific dataset.
        """
        df = df.withColumn("part_d", to_date(col("creation_ts_utc")))
        return df