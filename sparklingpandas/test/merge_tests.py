from sparklingpandas.test.sp_test_case import \
    SparklingPandasTestCase
	
from pandas.util.testing import assertRaises
	
class Merge(SparklingPandasTestCase):

	def merge_test_on(self):
        left_input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
		right_input = [("tea", "yummy"), ("water", "ok"), ("coffee", "meh")]
        left_dataframe = self.psc.DataFrame(left_input, columns=['thing', 'magic'])
		right_dataframe = self.psc.DataFrame(right_input, columns=['thing', 'magic'])
        merged = self.merge(left_dataframe, right_dataframe, on='thing').collect()
        assert len(merged.index) == 3
        expected = sorted(['thing', 'magic_x', 'magic_y'])
        assert sorted(merged.columns) == expected
		
	def merge_test_left_right_on(self):
        left_input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
		right_input = [("tea", "yummy"), ("water", "ok"), ("coffee", "meh")]
        left_dataframe = self.psc.DataFrame(left_input, columns=['thing', 'magic'])
		right_dataframe = self.psc.DataFrame(right_input, columns=['nothing', 'magic'])
        merged = self.merge(left_dataframe, right_dataframe, 
							left_on='thing', right_on='nothing').collect()
        assert len(merged.index) == 3
        expected = sorted(['thing', 'nothing', 'magic_x', 'magic_y'])
        assert sorted(merged.columns) == expected
		
	def merge_test_left_outer(self):
        left_input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
		right_input = [("tea", "yummy"), ("coffee", "meh")]
        left_dataframe = self.psc.DataFrame(left_input, columns=['thing', 'magic'])
		right_dataframe = self.psc.DataFrame(right_input, columns=['nothing', 'magic'])
        merged = self.merge(left_dataframe, right_dataframe, left_on='thing', 
							right_on='nothing', on='left').collect()
        assert len(merged.index) == 3
        expected = sorted(['thing', 'nothing', 'magic_x', 'magic_y'])
        assert sorted(merged.columns) == expected
		assert sorted(merged.ix[:,'nothing']) == sorted(['tea', 'coffee', None])
	
	def merge_test_right_outer(self):
		left_input = [("tea", "yummy"), ("coffee", "meh")]
        right_input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        left_dataframe = self.psc.DataFrame(left_input, columns=['thing', 'magic'])
		right_dataframe = self.psc.DataFrame(right_input, columns=['nothing', 'magic'])
        merged = self.merge(left_dataframe, right_dataframe, 
							left_on='thing', right_on='nothing', on='right').collect()
        assert len(merged.index) == 3
        expected = sorted(['thing', 'nothing', 'magic_x', 'magic_y'])
        assert sorted(merged.columns) == expected
		assert sorted(merged.ix[:,'thing']) == sorted(['tea', 'coffee', None])
		
	def test_merge_fails_with_different_column_counts(self):
        with assertRaises(ValueError):
            left_input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
			right_input = [("tea", "yummy"), ("water", "ok"), ("coffee", "meh")]
			left_dataframe = self.psc.DataFrame(left_input, columns=['thing', 'magic'])
			right_dataframe = self.psc.DataFrame(right_input, columns=['nothing', 'magic'])
            self.merge(left_dataframe, right_dataframe, 
						right_on='nothing', left_on=['thing', 'magic']).collect()
			
	def test_merge_fails_with_right_on_only(self):
		with assertRaises(MergeError):
			left_input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
			right_input = [("tea", "yummy"), ("water", "ok"), ("coffee", "meh")]
			left_dataframe = self.psc.DataFrame(left_input, columns=['thing', 'magic'])
			right_dataframe = self.psc.DataFrame(right_input, columns=['nothing', 'magic'])
			self.merge(left_dataframe, right_dataframe, right_on='thing').collect()
			
	def test_merge_fails_with_left_on_only(self):
		with assertRaises(MergeError):
			left_input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
			right_input = [("tea", "yummy"), ("water", "ok"), ("coffee", "meh")]
			left_dataframe = self.psc.DataFrame(left_input, columns=['thing', 'magic'])
			right_dataframe = self.psc.DataFrame(right_input, columns=['nothing', 'magic'])
			self.merge(left_dataframe, right_dataframe, left_on='thing').collect()
			
		