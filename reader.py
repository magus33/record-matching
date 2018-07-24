import csv
import itertools
import logging

from difflib import SequenceMatcher

from collections import defaultdict

class Reader(object):
    def __init__(self, filename, delim=','):
        self.rec_id = 0
        self._fo = open(filename, 'r', encoding='utf8')
        # self.num_records =
        self.reader = csv.DictReader(self._fo, delimiter=delim)

    def __iter__(self):
        return self

    def __next__(self):
        self.rec_id += 1
        return self.rec_id, self.reader.__next__()

    def close(self):
        self._fo.close()

class SubstrIndex(object):
    def __init__(self, dataset1, index_defn, rec_comparator):
        self.dataset = dataset1
        self.index_defn = index_defn
        self.datastore = {}
        self.rec_comparator = rec_comparator
        self.indexers = []


    def import_data(self,):


    def compress_data(self, ):
        """
        Takes entire row of data and stores only fields used in comparison
        :return: data store dictionary, { rec_id : list of fields }
        """
        reader = Reader(self.dataset)
        for (rec_id, row) in reader:
            fields = []
            for (field, comparator) in self.rec_comparator:
                fields.append(row[field])

            self.datastore[rec_id] = fields

    def build(self):
        """
        Builds indexer based on substring of field.  Successive indexes are stored as list.
        :return: list of indexers
        """
        reader = Reader(self.dataset)
        for (index_field, substr) in self.index_defn:
            indexer = defaultdict(list)

            for (rec_id, row) in reader:
                field = row[index_field]

                if not isinstance(field, str):
                    field = str(field)

                field_substr = field.strip().lower()[:substr]
                indexer[field_substr].append(rec_id)

        self.indexers.append(indexer)

    def run(self):
        """
        Compares records stored under index using record comparator
        :return: weight vector for storage
        """
        weight_vec_dict = {}
        for indexer in self.indexers:
            for field_substr in indexer.keys():
                rec_ids = indexer[field_substr]
                if len(rec_ids) > 1:
                    for (rec_id1, rec_id2) in itertools.combinations(rec_ids, 2):
                        weight_vec = []
                        for field_ind, ( _, field_comparator) in enumerate(self.rec_comparator):
                            rec1 = self.datastore[rec_id1][field_ind]
                            rec2 = self.datastore[rec_id2][field_ind]
                            w = field_comparator.compare(rec1, rec2)

                            weight_vec.append(w)
                            weight_vec_dict[(rec_id1, rec_id2)] = weight_vec

        return weight_vec_dict

class FieldComparator(object):
    def __init__(self, agree_weight=1, disagree_weight=0, missing_weight=0, thresh=0.7, raw_score=True, quick=False):
        self.agree_weight = agree_weight
        self.disagree_weight = disagree_weight
        self.missing_weight = missing_weight
        self.raw_score = raw_score
        self.thresh = thresh
        self.quick = quick

    def compare(self, val1, val2):
        ratio = SequenceMatcher(None, val1, val2).quick_ratio()

        if val1 == "" or val2 == "":
            return self.missing_weight

        if self.raw_score:
            return ratio
        else:
            if ratio >= self.thresh:
                return self.agree_weight
            else:
                return self.disagree_weight


# if __name__ == "__main__":
dataset = r'C:\Work\Data Sources\Schools\Directory-School-Current.csv'
# for (rowid, row) in dataset:
#     print(row)

field_comparator = FieldComparator(raw_score=True)
rec_comparator = (('Name', field_comparator),)

index_defn = (('Name', 2),)
indexer = SubstrIndex(dataset, index_defn, rec_comparator)

indexer.compress_data()
indexer.build()

weight_vec = indexer.run()

