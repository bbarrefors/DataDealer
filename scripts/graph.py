#!/usr/bin/python -B

"""
_CMSDataDealer_

Created by Bjorn Barrefors on 04/6/2014
for the CMS Data Dealer Agent

Holland Computing Center - University of Nebraska-Lincoln
"""
__author__ = 'Bjorn Barrefors'
__organization__ = 'Holland Computing Center - University of Nebraska-Lincoln'
__email__ = 'bbarrefo@cse.unl.edu'

import sys
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import datetime

values = (4, 10, 6, 8, 13, 15, 12, 18, 17, 20)
dates = ["2014-05-27", "2014-05-28", "2014-05-29", "2014-05-30", "2014-05-31", "2014-06-01", "2014-06-02", "2014-06-03", "2014-06-04", "2014-06-05"]

#x_pos = np.arange((len(dates)))
x_pos = np.linspace(0, len(dates), len(dates))
y_pos = [int(pos) for pos in np.linspace(0, (max(values)*1.05), 5)]
print x_pos
plt.bar(x_pos, values, align='center')
plt.xticks(x_pos, dates, rotation=20)

plt.yticks(y_pos)
plt.ylabel('Accesses')
plt.title('')

plt.savefig("d1.eps", format="eps")