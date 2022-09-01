import numpy as np

class ConfusionMatrixManager:

    def __init__(self, model_no, labels):
        self.model_no = model_no
        self.labels = labels

    def table_divide_and_format(self, table):
        # TODO: can it be more optimized that row by row np.array conversion
        # This method divides table into actual labels list and probility values
        if len(table) == 0:
            print("INFO: Table lenght is 0!")
        else:
            self.given_label_list = np.array(list([row[1] for row in table]))
            self.prob_values = [np.array(list(map(float,row[2:len(row)])), dtype = np.float32) for row in table]
  
    def calculate_avg_preds(self):
        # TODO: make it scalable for n models
        np_table = np.array(self.prob_values)
        w1, w2, w3 = 0.5, 0.6, 0.7
        weights_label_a = np.array([w1, 0, w2, 0, w3, 0])
        weights_label_b = np.array([0, w1, 0, w2, 0, w3])

        self.avg_probs_a = np.sum(np_table*weights_label_a, axis=1)/3
        self.avg_probs_b = np.sum(np_table*weights_label_b, axis=1)/3

    def calculate_confusion_matrix(self):
        actual = self.given_label_list
        preds = self.pred_label_list
        labels = np.unique(actual)
        matrix = np.zeros((len(labels), len(labels)))

        for i in range(len(labels)):
            for j in range(len(labels)):
                matrix[i,j] = np.sum( (actual==labels[i]) & (preds==labels[j]))
        return matrix
    
    def calculate_pred_list(self):
        res = self.avg_probs_a - self.avg_probs_b
        self.pred_label_list = np.array(["A" if x>=0 else "B" for x in res])
   