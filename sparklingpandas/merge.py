"""Provide wrapper around the merged result of two L{DataFrame}s"""
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from sparklingpandas.utils import add_pyspark_path
from sparklingpandas.dataframe import DataFrame
add_pyspark_path()
import pyspark
from pyspark.sql.column import Column, _to_java_column
import pandas.core.common as com

def merge(left, right, how='inner', on=None, left_on=None, right_on=None,
          left_index=False, right_index=False, sort=False,
          suffixes=('_x', '_y'), copy=True):
		  
	joined = _MergeOperation(left=left, right=right, how=how, on=on, 
					left_on=left_on, right_on=right_on, left_index=left_index,
					right_index=right_index, sort=sort, suffixes=suffixes, 
					copy=copy)
	return joined.get_result()	

class MergeError(ValueError):
    pass		
	
class _MergeOperation:
		def __init__(self, left, right, how='inner', on=None,
                 left_on=None, right_on=None, axis=1,
                 left_index=False, right_index=False, sort=True,
                 suffixes=('_x', '_y'), copy=True):
			"""Construct a join object providing the functions on top 
			of the provided RDD. """
			self.left = left
			self.right = right
			self.how = self._map_how(how)
			self.axis = axis

			self.on = com._maybe_make_list(on)
			self.left_on = com._maybe_make_list(left_on)
			self.right_on = com._maybe_make_list(right_on)

			self.copy = copy
			self.suffixes = suffixes
			self.sort = sort

			self.left_index = left_index
			self.right_index = right_index
			
		def _map_how(self, pandas_how):
			how_map = {'inner': 'inner', 'outer': 'outer', 
					'left': 'left_outer', 'right': 'right_outer'}
			if pandas_how in how_map.keys():
				return how_map[pandas_how]
			else:
				raise MergeError('how must be "right", "left", '
						'"inner", or "outer"')
			
		def _columns_without_key(self):
			def remove_columns(col_list, remove_list):
				return [col for col in col_list if col not in remove_list]
				
			left_columns = self.left.to_spark_sql().columns
			right_columns = self.right.to_spark_sql().columns
			if self.on is not None:
				left_columns = remove_columns(left_columns, self.on)
				right_columns = remove_columns(right_columns, self.on)
			else:
				left_columns = remove_columns(left_columns, self.left_on)
				right_columns = remove_columns(right_columns, self.right_on)
			return left_columns, right_columns
			
		def _validate_specification(self):
		#currently spark only supports 2 scenarios: 
		#1. join using left_on/right_on where the key names are different
		#2. join using on where there is only one key
		#spark cannot support a list of keys using on or joining without 
		#specifying a key
			if((self.left_on is not None or self.right_on is not None) 
					and self.on is not None):
				raise MergeError('Can only pass on OR left_on and '
                                 'right_on')
			if self.left_on:
				if len(self.left_on) != len(self.right_on):
					raise ValueError('len(right_on) must equal the number '
							'of levels in the index of "left"')
			if (self.right_on is None and self.left_on is None 
					and self.on is None):
				raise MergeError('Spark does not support this until '
					'version 1.6. Please specify key names')
			if (self.on is not None and len(self.on) > 1):
				raise MergeError('Spark does not support this until '
					'version 1.6')
			left_columns, right_columns = self._columns_without_key()
			def pick_not_none(x, y):
				#return either one of the two that is not None
				#however, if both are none, still return None
				if x is None:
					return y
				else:
					return x
					
			left_key = pick_not_none(self.left_on, self.on)
			right_key = pick_not_none(self.right_on, self.on)
			if(right_key in right_columns or left_key in left_columns):
				raise ValueError('Buffer has wrong number of dimensions' 
								'(expected 1, got 2)')
								
		def _prep_for_merge(self):
			def find_common_cols(left_columns, right_columns):
				return list(set(left_columns).intersection(right_columns))
				
			left_columns, right_columns = self._columns_without_key()
			common_cols = find_common_cols(left_columns, right_columns)		
			left_rdd = self.left.to_spark_sql()
			right_rdd = self.right.to_spark_sql()
			def add_suffix(suffix, df, common_cols):
				cols = [Column(_to_java_column(c)).alias(c+suffix)
						if c in common_cols else c
						for c in df.columns]
				return df.select(*cols)
				
			left_rdd_with_suffixes = add_suffix(self.suffixes[0], 
					left_rdd, common_cols)
			right_rdd_with_suffixes = add_suffix(self.suffixes[1], 
					right_rdd, common_cols)
			return left_rdd_with_suffixes, right_rdd_with_suffixes	
		
		def _join_condition(left_rdd, right_rdd, left_on, right_on):
			def create_condition(left_rdd, right_rdd, left_on, right_on):
				return getattr(left_rdd, left_on) == \
						getattr(right_rdd, right_on)
							
			condition = create_condition(left_rdd, right_rdd, 
										self.left_on[0], self.right_on[0])
			for (a,b) in enumerate(zip(self.left_on[1:], self.right_on[1:])):
				condition = condition and create_condition(left_rdd, right_rdd,
						a, b) 
			return condition
			
		def get_result(self):
			def list_head(some_list):
				if some_list:
					return some_list[0]
					
			self._validate_specification()
			left_rdd_with_suffixes, \
					right_rdd_with_suffixes = self._prep_for_merge()
			if self.on is not None:
				joined = left_rdd_with_suffixes.join(right_rdd_with_suffixes, 
						list_head(self.on), self.how)
			else:
				joined = left_rdd_with_suffixes.\
					join(right_rdd_with_suffixes,
							_join_condition(left_rdd_with_suffixes, 
									right_rdd_with_suffixes, self.left_on,
									self.right_on), self.how)
			if self.sort:
				#according to spark documentation, we can only sort
				#by one column
				if self.on:
					joined = joined.sort(self.on[0])
				else:
					joined = joined.sort(self.left_on[0])
			return DataFrame.from_schema_rdd(joined)
				