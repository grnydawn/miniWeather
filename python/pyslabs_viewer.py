import os, sys

import matplotlib.pyplot as plt
import pyslabs
import numpy as np

def main():

    # load
    with pyslabs.master_open(sys.argv[1]) as data:

        # process
        #dens_view = data.get_view("dens")
        dens_array = data.get_array("dens")
        #dens_view.write(dens_modified_array)

        #import pdb; pdb.set_trace()
        cs = plt.contourf(dens_array[0, :, :])
        plt.savefig("test0.png")
        cs = plt.contourf(dens_array[1, :, :])
        plt.savefig("test1.png")
        cs = plt.contourf(dens_array[2, :, :])
        plt.savefig("test2.png")

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


if __name__ == "__main__":
    sys.exit(main())
