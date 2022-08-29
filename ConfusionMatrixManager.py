
import pandas as pd
from time import sleep, perf_counter
from threading import Thread
import sqlite3
import csv
import numpy as np

class ConfusionMatrixManager:

    given_label_list = None
    prob_values = None
    avg_probs_a = None
    avg_probs_b = None
    pred_label_list = None

    def __init__(self, table, model_no, labels):
        self.table = table
        self.model_no = model_no
        self.labels = labels
        print("Model initialized!")

    def table_divide_and_format(self):
        # TODO: can it be more optimized that row by row np.array conversion
        # info: divides table into actual labels list and probility values
        self.given_label_list = np.array(list([row[1] for row in self.table]))
        self.prob_values = [np.array(list(map(float,row[2:len(row)])), dtype = np.float32) for row in self.table]
        #print("TEST 0 - table test: ", self.table)

    def get_avg_preds(self):
        # TODO: make it scalable for n models
        np_table = np.array(self.prob_values)
        w1, w2, w3 = 1,1,1
        #0.5, 0.6, 0.7
        weights_label_a = np.array([w1, 0, w2, 0, w3, 0])
        weights_label_b = np.array([0, w1, 0, w2, 0, w3])

        self.avg_probs_a = np.sum(np_table*weights_label_a, axis=1)/3
        self.avg_probs_b = np.sum(np_table*weights_label_b, axis=1)/3
     
    def confusion_matrix(self):
        actual = self.given_label_list
        preds = self.pred_label_list

        labels = np.unique(actual)
        matrix = np.zeros((len(labels), len(labels)))
        print("Labels: ",labels)
        print("actual: ", actual)
        print("preds: ", preds)
        for i in range(len(labels)):
            for j in range(len(labels)):
                matrix[i,j] = np.sum( (actual==labels[i]) & (preds==labels[j]))
        return matrix

    def get_pred_list(self):
        res = self.avg_probs_a - self.avg_probs_b
        self.pred_label_list = np.array(["A" if x>=0 else "B" for x in res])

# Sample ConfusionMatrixManager method
# sample_table = [('2', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
#                 ('3', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
#                 ('4', 'B', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
#                 ('5', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
#                 ('6', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0')]

# cmm = ConfusionMatrixManager(sample_table, 3, ["A", "B"])
# cmm.table_divide_and_format()
# cmm.get_avg_preds()
# cmm.get_pred_list()

