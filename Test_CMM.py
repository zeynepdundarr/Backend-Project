from email import message
import unittest
from DataSourceManager import DataSourceManager
from ConfusionMatrixManager import ConfusionMatrixManager
import pandas as pd
import numpy as np

class Test_CMM(unittest.TestCase):

    def setUp(self):
        self.model_no = 3
        self.labels = ["A", "B"]
        self.cmm = ConfusionMatrixManager(self.model_no, self.labels)
        self.table = [(1, 'A', 0.6315429094436551, 0.3684570905563449, 0.9881789935400176, 0.0118210064599824, 0.7254980531654877, 0.2745019468345123), 
            (2, 'B', 0.7293816120747935, 0.2706183879252065, 0.0101061883365132, 0.9898938116634868, 0.7539505178154802, 0.2460494821845198), 
            (3, 'A', 0.979591138558099, 0.020408861441901, 0.9205375921041954, 0.0794624078958046, 0.0339680584508004, 0.9660319415491996), 
            (4, 'B', 0.0890861149945974, 0.9109138850054026, 0.7272518420144608, 0.2727481579855392, 0.9409147314255956, 0.0590852685744043), 
            (5, 'B', 0.4501389662697976, 0.5498610337302023, 0.1164488426470428, 0.8835511573529572, 0.2195979863755559, 0.7804020136244441), 
            (6, 'B', 0.8624519236918461, 0.1375480763081539, 0.9840406330873848, 0.0159593669126153, 0.2189237185835933, 0.7810762814164066), 
            (7, 'A', 0.6078003271789233, 0.3921996728210767, 0.8420289090983645, 0.1579710909016355, 0.2798621029211228, 0.7201378970788772), 
            (8, 'A', 0.137643014118616, 0.862356985881384, 0.3866563830533508, 0.6133436169466492, 0.6355385487225098, 0.3644614512774902), 
            (9, 'B', 0.0340706562170941, 0.9659293437829058, 0.1680376539732921, 0.8319623460267078, 0.6356226404777658, 0.3643773595222341), 
            (10, 'B', 0.5094794855363648, 0.4905205144636352, 0.2908688646922155, 0.7091311353077845, 0.0450495751970624, 0.9549504248029376)]
        self.row_count = len(self.table)
        self.col_count = len(self.table[0][2:])
        self.prob_dim = 2
        self.conf_dim = 2
        self.given_labels_dim = 1

    def test_table_divide_and_format(self): 
        prob_values_modified = np.array([np.array(list(map(float,row[2:len(row)])), dtype = np.float32) for row in self.table])
        check_given_label_values = False
        self.cmm.table_divide_and_format(self.table)
        given_label_set = set(self.cmm.given_label_list)
        given_label_size_flag = False
        prob_size_flag = False
        
        if self.cmm.given_label_list.ndim == self.given_labels_dim:
            for i in self.labels:
                if i in given_label_set:
                    check_given_label_values = True
                    if len(self.cmm.given_label_list) == self.row_count:
                        given_label_size_flag = True
                else:
                    check_given_label_values = False

        if prob_values_modified.ndim == self.prob_dim :
            prob_dim_flag = True
            if len(prob_values_modified) == self.row_count:
                if len(prob_values_modified[0]) == self.col_count:
                    prob_size_flag = True
            else:
                prob_size_flag = False
        else:
            prob_dim_flag = False
        message = "Probability values type or given label values are incorrect!"
        self.assertTrue(prob_size_flag & prob_dim_flag & check_given_label_values & given_label_size_flag, msg=message)

    def test_calculate_avg_preds(self):
        self.cmm.table_divide_and_format(self.table)
        self.cmm.calculate_avg_preds()
        pred_a = self.cmm.avg_probs_a
        pred_b = self.cmm.avg_probs_b
        pred_size_flag = False

        if len(pred_a) == self.row_count & len(pred_b) == self.row_count:
            pred_size_flag = True
        message = "Pred_a or pred_b size is incorrect!"
        self.assertTrue(pred_size_flag, msg=message)
    
    def test_calculate_confusion_matrix(self):
        self.cmm.table_divide_and_format(self.table)
        self.cmm.calculate_avg_preds()
        self.cmm.calculate_pred_list()
        
        conf_mat = self.cmm.calculate_confusion_matrix()
        conf_matrix_size_flag = False
        if len(conf_mat) == self.conf_dim and len(conf_mat[0]) == self.conf_dim:
            conf_matrix_size_flag = True
        self.assertTrue(conf_matrix_size_flag, msg=message)
    
    def test_calculate_pred_list(self):
        self.cmm.table_divide_and_format(self.table)
        self.cmm.calculate_avg_preds()
        self.cmm.calculate_pred_list()
        pred_label_list = self.cmm.pred_label_list
        pred_label_list_flag = False

        if len(pred_label_list) == self.row_count:
            pred_label_list_flag = True
        
        pred_label_list_content_flag = False
        for i in set(pred_label_list):
            if i in self.labels:
                pred_label_list_content_flag = True
            else:
                pred_label_list_content_flag = False
        message = "pred_label_list_flag size or content is incorrect!"

        print("pred_label_list_flag: ",pred_label_list_flag)
        print("pred_label_list_content_flag: ", pred_label_list_content_flag)

        self.assertTrue(pred_label_list_flag&pred_label_list_content_flag, msg=message)

if __name__ == '__main__':
    unittest.main()


    

