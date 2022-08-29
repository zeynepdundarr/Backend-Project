import numpy as np

class ConfusionMatrixManager:

    given_label_list = None
    prob_values = None
    avg_probs_a = None
    avg_probs_b = None
    pred_label_list = None

    def __init__(self, model_no, labels):
        self.model_no = model_no
        self.labels = labels

    def table_divide_and_format(self, table):
        # TODO: can it be more optimized that row by row np.array conversion
        # info: divides table into actual labels list and probility values
        self.given_label_list = np.array(list([row[1] for row in table]))
        self.prob_values = [np.array(list(map(float,row[2:len(row)])), dtype = np.float32) for row in table]
        # print("TEST 2 -  self.prob_values: ", self.prob_values)
        # print("TEST 2 -  self.given_label_list: ", self.prob_values)
    
    def calculate_avg_preds(self):
        # TODO: make it scalable for n models
        np_table = np.array(self.prob_values)
        w1, w2, w3 = 1,1,1
        #0.5, 0.6, 0.7
        weights_label_a = np.array([w1, 0, w2, 0, w3, 0])
        weights_label_b = np.array([0, w1, 0, w2, 0, w3])

        self.avg_probs_a = np.sum(np_table*weights_label_a, axis=1)/3
        self.avg_probs_b = np.sum(np_table*weights_label_b, axis=1)/3
        # print("TEST 0 -  self.avg_probs_a: ", self.avg_probs_a)
        # print("TEST 0 -  self.avg_probs_b: ", self.avg_probs_b)
     
    def calculate_confusion_matrix(self):
        actual = self.given_label_list
        preds = self.pred_label_list
        labels = np.unique(actual)
        matrix = np.zeros((len(labels), len(labels)))
        # print("Labels: ",labels)
        # print("actual: ", actual)
        # print("preds: ", preds)

        for i in range(len(labels)):
            for j in range(len(labels)):
                matrix[i,j] = np.sum( (actual==labels[i]) & (preds==labels[j]))
        return matrix
    
    def calculate_pred_list(self):
        res = self.avg_probs_a - self.avg_probs_b
        self.pred_label_list = np.array(["A" if x>=0 else "B" for x in res])
        #print("TEST 1 - calculate_pred_list: ", self.pred_label_list)
