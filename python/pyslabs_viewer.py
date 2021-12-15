import os, sys

import matplotlib.pyplot as plt
import simpdf
import numpy as np

_data = "/gpfs/alpine/cli133/scratch/grnydawn/output"

def main():

    # load
    with simpdf.master_open(_data) as data:

        # process
        dens = data.get_var("dens")

        cs = plt.contourf(dens)
#        x = np.arange(1, 10)
#        y = x.reshape(-1, 1)
#        h = x * y
#
#        cs = plt.contourf(h, levels=[10, 30, 50],
#            colors=['#808080', '#A0A0A0', '#C0C0C0'], extend='both')
#        cs.cmap.set_over('red')
#        cs.cmap.set_under('blue')
#        cs.changed()
        #plt.show()
        plt.savefig("test.png")


if __name__ == "__main__":
    sys.exit(main())
