import os, sys, argparse, time, math, numpy
from mpi4py import MPI
import pyslabs

NX = 100 # 2000 # 100            # number of local grid cells in the x-dimension
NZ = 50 # 1000 # 50             # number of local grid cells in the z-dimension
SIM_TIME = 10 # 5 # 10     # total simulation time in seconds
OUT_FREQ = 5 # 5 # 10       # frequency to perform output in seconds
DATA_SPEC = "DATA_SPEC_THERMAL" # which data initialization to use
NUM_VARS = 4        # number of fluid state variables

class LocalDomain():
    """a local domain that has spatial state and computation of the domain
    """

    xlen = 2.E4 # Length of the domain in the x-direction (meters)
    zlen = 1.E4 # Length of the domain in the z-direction (meters)

    max_speed = 450. # Assumed maximum wave speed during the simulation
                    # (speed of sound + speed of wind) (meter / sec)
    hv_beta = 0.25 # How strong to diffuse the solution: hv_beta \in [0:1]

    cfl = 1.50 # Courant, Friedrichs, Lewy" number (for numerical stability)
    sten_size = 4 # Size of the stencil used for interpolation
    hs = 2  #  "Halo" size: number of cells beyond the MPI tasks's domain
            # needed for a full "stencil" of information for reconstruction

    ID_DENS = 0 # index for density ("rho")
    ID_UMOM = 1 # index for momentum in the x-direction ("rho * u")
    ID_WMOM = 2 # index for momentum in the z-direction ("rho * w")
    ID_RHOT = 3 # index for density * potential temperature ("rho * theta")

    DIR_X = 0 # Integer constant to express that this operation is in the x-direction
    DIR_Z = 1 # Integer constant to express that this operation is in the z-direction

    # Gauss-Legendre quadrature points and weights on the domain [0:1]
    nqpoints = 3
    qpoints = (0.112701665379258311482073460022, 0.500000000000000000000000000000, 0.887298334620741688517926539980)
    qweights = (0.277777777777777777777777777779, 0.444444444444444444444444444444, 0.277777777777777777777777777779)

    pi = 3.14159265358979323846264338327 # Pi
    grav = 9.8 # Gravitational acceleration (m / s^2)
    cp = 1004. # Specific heat of dry air at constant pressure
    cv = 717. # Specific heat of dry air at constant volume
    rd = 287. # Dry air constant for equation of state (P=rho*rd*T)
    p0 = 1.e5 # Standard pressure at the surface in Pascals
    c0 = 27.5629410929725921310572974482 # Constant to translate potential temperature
                                         # into pressure (P=C0*(rho*theta)**gamma)
    gamma = 1.40027894002789400278940027894 #gamma=cp/Rd

    theta0 = 300. # Background potential temperature
    exner0 = 1. # Surface-level Exner pressure


    def __init__(self, nx_glob, nz_glob, data_spec):

        self.comm = MPI.COMM_WORLD
        self.nranks = self.comm.Get_size()
        self.rank = self.comm.Get_rank()

        self.data_spec_int = data_spec
        self.nx_glob = nx_glob
        self.nz_glob = nz_glob
        self.nz = self.nz_glob

        self.dx = self.xlen / nx_glob
        self.dz = self.zlen / nz_glob

        nper = float(nx_glob) / self.nranks
        self.i_beg = round(nper * self.rank)
        self.i_end = round(nper * (self.rank + 1))
        self.nx = self.i_end - self.i_beg

        self.left_rank = self.nranks-1 if self.rank==0 else self.rank-1
        self.right_rank = 0 if self.rank==self.nranks-1 else self.rank+1

        self.k_beg = 0

        self.dt = min(self.dx, self.dz) / self.max_speed * self.cfl

        state_shape = (self.nx + self.hs*2, self.nz + self.hs*2, NUM_VARS)
        self.state = numpy.zeros(state_shape, order="F", dtype=numpy.float64)
        self.state_tmp = numpy.empty(state_shape, order="F", dtype=numpy.float64)

        buf_shape = (self.hs, self.nz, NUM_VARS)
        self.sendbuf_l = numpy.empty(buf_shape, order="F", dtype=numpy.float64)
        self.sendbuf_r = numpy.empty(buf_shape, order="F", dtype=numpy.float64)
        self.recvbuf_l = numpy.empty(buf_shape, order="F", dtype=numpy.float64)
        self.recvbuf_r = numpy.empty(buf_shape, order="F", dtype=numpy.float64)

        self.flux = numpy.zeros((self.nx+1, self.nz+1, NUM_VARS), order="F", dtype=numpy.float64)
        self.tend = numpy.zeros((self.nx, self.nz, NUM_VARS), order="F", dtype=numpy.float64)

        self.hy_dens_cell = numpy.zeros(self.nz+self.hs*2, dtype=numpy.float64)
        self.hy_dens_theta_cell = numpy.zeros(self.nz+self.hs*2, dtype=numpy.float64)

        self.hy_dens_int = numpy.empty(self.nz+1, dtype=numpy.float64)
        self.hy_dens_theta_int = numpy.empty(self.nz+1, dtype=numpy.float64)
        self.hy_pressure_int = numpy.empty(self.nz+1, dtype=numpy.float64)

        self.stencil = numpy.empty(self.sten_size, dtype=numpy.float64)
        self.d3_vals = numpy.empty(NUM_VARS, dtype=numpy.float64)
        self.vals = numpy.empty(NUM_VARS, dtype=numpy.float64)

        self.dens = numpy.empty((self.nx, self.nz), dtype=numpy.float64)
        self.uwnd = numpy.empty((self.nx, self.nz), dtype=numpy.float64)
        self.wwnd = numpy.empty((self.nx, self.nz), dtype=numpy.float64)
        self.theta = numpy.empty((self.nx, self.nz), dtype=numpy.float64)

        if self.is_master():
            print("nx_glob=%d, nz_glob=%d" % (self.nx_glob, self.nz_glob))
            print("dx=%f, dz=%f, dt=%f" % (self.dx, self.dz, self.dt))

        self.comm.Barrier()

        # Initialize the cell-averaged fluid state via Gauss-Legendre quadrature
        for k in range(self.nz+2*self.hs):
            for i in range(self.nx+2*self.hs):
                for kk in range(self.nqpoints):
                    for ii in range(self.nqpoints):
                        x = ((self.i_beg + i - self.hs + 0.5) * self.dx + 
                                (self.qpoints[ii] - 0.5) * self.dx)
                        z = ((self.k_beg + k - self.hs + 0.5) * self.dz + 
                                (self.qpoints[kk] - 0.5) * self.dx)

                        if self.data_spec_int == "DATA_SPEC_COLLISION":
                            r, u, w, t, hr, ht = self.collision(x, z)
                        elif self.data_spec_int == "DATA_SPEC_THERMAL": 
                            r, u, w, t, hr, ht = self.thermal(x, z)

                        self.state[i, k, self.ID_DENS] += r * self.qweights[ii] * self.qweights[kk]
                        self.state[i, k, self.ID_UMOM] += (r+hr)*u * self.qweights[ii] * self.qweights[kk]
                        self.state[i, k, self.ID_WMOM] += (r+hr)*w * self.qweights[ii] * self.qweights[kk]
                        self.state[i, k, self.ID_RHOT] += (((r+hr)*(t+ht) - hr*ht) * self.qweights[ii] *
                                                            self.qweights[kk])

                for ll in range(NUM_VARS):
                    self.state_tmp[i,k,ll] = self.state[i,k,ll]

        # Compute the hydrostatic background state over vertical cell averages
        for k in range(self.nz+self.hs*2):
            for kk in range(self.nqpoints):
                z = (self.k_beg + k - 1.5) * self.dz + (self.qpoints[kk] -0.5) * self.dz
                if self.data_spec_int == "DATA_SPEC_COLLISION":
                    r, u, w, t, hr, ht = self.collision(0., z)
                elif self.data_spec_int == "DATA_SPEC_THERMAL": 
                    r, u, w, t, hr, ht = self.thermal(0., z)

                self.hy_dens_cell[k]       += hr * self.qweights[kk]
                self.hy_dens_theta_cell[k] += hr*ht * self.qweights[kk]

        # Compute the hydrostatic background state at vertical cell interfaces
        for k in range(self.nz+1):
            z = (self.k_beg + k) * self.dz
            if self.data_spec_int == "DATA_SPEC_COLLISION":
                r, u, w, t, hr, ht = self.collision(0., z)
            elif self.data_spec_int == "DATA_SPEC_THERMAL": 
                r, u, w, t, hr, ht = self.thermal(0., z)

            self.hy_dens_int[k]       = hr
            self.hy_dens_theta_int[k] = hr * ht
            self.hy_pressure_int[k] = self.c0 * math.pow(hr*ht, self.gamma)

        try:
            # create file
            if self.is_master():

                self.slabs = pyslabs.master_open("/gpfs/alpine/cli133/scratch/grnydawn/output", mode="w")

                self.vdens = self.slabs.define_var("dens")
                self.vuwnd = self.slabs.define_var("uwnd")
                self.vwwnd = self.slabs.define_var("wwnd")
                self.vtheta = self.slabs.define_var("theta")

                self.slabs.begin()

            else:
                # wait master
                self.slabs = pyslabs.parallel_open("/gpfs/alpine/cli133/scratch/grnydawn/output", mode="w")

                # get to ndarray dimension
                self.vdens = self.slabs.get_var("dens")
                self.vuwnd = self.slabs.get_var("uwnd")
                self.vwwnd = self.slabs.get_var("wwnd")
                self.vtheta = self.slabs.get_var("theta")

        except Exception as err:
            print("ERROR: %s" % str(err))
            self.comm.Abort(-1)

    def set_halo_values_z(self, state):

        for ll in range(NUM_VARS):
            for i in range(self.nx+self.hs*2):
                for h in range(self.hs):
                    if ll == self.ID_WMOM:
                        state[i, h, ll] = 0.
                        state[i, -h, ll] = 0.
                    else:
                        state[i, h, ll] = state[i, self.hs, ll]
                        state[i, -h, ll] = state[i, -self.hs, ll]
#
#                if ll == self.ID_WMOM:
#                    state[i, 0, ll] = 0.
#                    state[i, 1, ll] = 0.
#                    state[i, self.nz+self.hs, ll] = 0.
#                    state[i, self.nz+self.hs+1, ll] = 0.
#                else:
#                    state[i, 0, ll] = state[i, self.hs, ll]
#                    state[i, 1, ll] = state[i, self.hs, ll]
#                    state[i, self.nz+self.hs, ll] = state[i, self.nz+self.hs-1, ll]
#                    state[i, self.nz+self.hs+1, ll] = state[i, self.nz+self.hs-1, ll]

    def compute_tendencies_z(self, state):
        # Compute the hyperviscosity coeficient
        hv_coef = -self.hv_beta * self.dz / (16. * self.dt)

        # Compute fluxes in the x-direction for each cell
        for k in range(self.nz+1):
            for i in range(self.nx):
                # Use fourth-order interpolation from four cell averages
                # to compute the value at the interface in question
                for ll in range(NUM_VARS):
                    for s in range(self.sten_size):
                        self.stencil[s] = state[i + self.hs, k + s, ll]

                    self.vals[ll] = (-self.stencil[0]/12. + 7.*self.stencil[1]/12. +
                                7.*self.stencil[2]/12. - self.stencil[3]/12.)

                    self.d3_vals[ll] = (-self.stencil[0] + 3.*self.stencil[1] -
                                    3.*self.stencil[2] + self.stencil[3])

                # Compute density, u-wind, w-wind, potential temperature,
                # and pressure (r,u,w,t,p respectively)

                r = self.vals[self.ID_DENS] + self.hy_dens_int[k]
                u = self.vals[self.ID_UMOM] / r
                w = self.vals[self.ID_WMOM] / r
                t = (self.vals[self.ID_RHOT] + self.hy_dens_theta_int[k] ) / r
                p = self.c0*math.pow(r*t,self.gamma) - self.hy_pressure_int[k]

                # Enforce vertical boundary condition and exact mass conservation
                if k == 0 or k == self.nz:
                    w = 0.
                    self.d3_vals[self.ID_DENS] = 0.

                #Compute the flux vector
                self.flux[i,k,self.ID_DENS] = r*w     - hv_coef*self.d3_vals[self.ID_DENS]
                self.flux[i,k,self.ID_UMOM] = r*w*u   - hv_coef*self.d3_vals[self.ID_UMOM]
                self.flux[i,k,self.ID_WMOM] = r*w*w+p - hv_coef*self.d3_vals[self.ID_WMOM]
                self.flux[i,k,self.ID_RHOT] = r*w*t   - hv_coef*self.d3_vals[self.ID_RHOT]

        for ll in range(NUM_VARS):
            for k in range(self.nz):
                for i in range(self.nx):
                    self.tend[i, k, ll] = -(self.flux[i, k+1, ll] - self.flux[i, k, ll]) / self.dz
                    if ll == self.ID_WMOM:
                        self.tend[i, k, self.ID_WMOM] = (self.tend[i, k, self.ID_WMOM] -
                                        state[i+self.hs, k+self.hs, self.ID_DENS] * self.grav)
              

    def set_halo_values_x(self, state):

        recvs = []
        recvs.append(self.comm.Irecv(self.recvbuf_l, self.left_rank, 0))
        recvs.append(self.comm.Irecv(self.recvbuf_r, self.right_rank, 1))

        for ll in range(NUM_VARS):
            for k in range(self.nz):
                for s in range(self.hs):
                    self.sendbuf_l[s, k, ll] = state[s+self.hs, k+self.hs, ll]
                    self.sendbuf_r[s, k, ll] = state[s-2*self.hs, k+self.hs, ll]

        sends = []
        sends.append(self.comm.Isend(self.sendbuf_l, self.left_rank, 1))
        sends.append(self.comm.Isend(self.sendbuf_r, self.right_rank, 0))

        MPI.Request.Waitall(recvs)

        for ll in range(NUM_VARS):
            for k in range(self.nz):
                for s in range(self.hs):
                    state[s, k+self.hs, ll] = self.recvbuf_l[s, k, ll]
                    state[self.nx+self.hs+s, k+self.hs, ll] = self.recvbuf_r[s, k, ll]

        MPI.Request.Waitall(sends)

    def compute_tendencies_x(self, state):
        # Compute the hyperviscosity coeficient
        hv_coef = -self.hv_beta * self.dx / (16. * self.dt)

        # Compute fluxes in the x-direction for each cell
        for k in range(self.nz):
            for i in range(self.nx + 1):
                # Use fourth-order interpolation from four cell averages
                # to compute the value at the interface in question
                for ll in range(NUM_VARS):
                    for s in range(self.sten_size):
                        self.stencil[s] = state[i + s, k+self.hs, ll]

                    self.vals[ll] = (-self.stencil[0]/12. + 7.*self.stencil[1]/12. +
                                7.*self.stencil[2]/12. - self.stencil[3]/12.)
                    self.d3_vals[ll] = (-self.stencil[0] + 3.*self.stencil[1] -
                                    3.*self.stencil[2] + self.stencil[3])

                # Compute density, u-wind, w-wind, potential temperature,
                # and pressure (r,u,w,t,p respectively)

                r = self.vals[self.ID_DENS] + self.hy_dens_cell[k+self.hs]
                u = self.vals[self.ID_UMOM] / r
                w = self.vals[self.ID_WMOM] / r
                t = (self.vals[self.ID_RHOT] + self.hy_dens_theta_cell[k+self.hs] ) / r
                p = self.c0*math.pow(r*t, self.gamma)

                #Compute the flux vector
                self.flux[i,k,self.ID_DENS] = r*u     - hv_coef*self.d3_vals[self.ID_DENS]
                self.flux[i,k,self.ID_UMOM] = r*u*u+p - hv_coef*self.d3_vals[self.ID_UMOM]
                self.flux[i,k,self.ID_WMOM] = r*u*w   - hv_coef*self.d3_vals[self.ID_WMOM]
                self.flux[i,k,self.ID_RHOT] = r*u*t   - hv_coef*self.d3_vals[self.ID_RHOT]

        for ll in range(NUM_VARS):
            for k in range(self.nz):
                for i in range(self.nx):
                    self.tend[i, k, ll] = -(self.flux[i+1, k, ll] - self.flux[i, k, ll]) / self.dx

    def semi_discrete_step(self, state_init, state_forcing, state_out, dt, dir):

        if dir == self.DIR_X:
            self.set_halo_values_x(state_forcing)
            self.compute_tendencies_x(state_forcing)
        elif dir == self.DIR_Z:
            self.set_halo_values_z(state_forcing)
            self.compute_tendencies_z(state_forcing)

        for ll in range(NUM_VARS):
            for k in range(self.nz):
                for i in range(self.nx):
                    state_out[i+self.hs, k+self.hs, ll] = (state_init[i+self.hs, k+self.hs, ll] +
                                                            dt * self.tend[i, k, ll])

    def timestep(self):

        direction_switch = True

        if direction_switch:
            # x direction first
            self.semi_discrete_step(self.state, self.state, self.state_tmp,
                                    self.dt / 3., self.DIR_X)
            self.semi_discrete_step(self.state, self.state_tmp, self.state_tmp,
                                    self.dt / 2., self.DIR_X)
            self.semi_discrete_step(self.state, self.state_tmp, self.state,
                                    self.dt / 1., self.DIR_X)


            # z direction second
            self.semi_discrete_step(self.state, self.state, self.state_tmp,
                                    self.dt / 3., self.DIR_Z)
            self.semi_discrete_step(self.state, self.state_tmp, self.state_tmp,
                                    self.dt / 2., self.DIR_Z)
            self.semi_discrete_step(self.state, self.state_tmp, self.state,
                                    self.dt / 1., self.DIR_Z)

        else:
            # z direction first
            self.semi_discrete_step(self.state, self.state, self.state_tmp,
                                    self.dt / 3., self.DIR_Z)
            self.semi_discrete_step(self.state, self.state_tmp, self.state_tmp,
                                    self.dt / 2., self.DIR_Z)
            self.semi_discrete_step(self.state, self.state_tmp, self.state,
                                    self.dt / 1., self.DIR_Z)

            # x direction second
            self.semi_discrete_step(self.state, self.state, self.state_tmp,
                                    self.dt / 3., self.DIR_X)
            self.semi_discrete_step(self.state, self.state_tmp, self.state_tmp,
                                    self.dt / 2., self.DIR_X)
            self.semi_discrete_step(self.state, self.state_tmp, self.state,
                                    self.dt / 1., self.DIR_X)

    def reductions(self):

        rdc = numpy.zeros(2, dtype=numpy.float64) # mass, te
        buf = numpy.empty(2, dtype=numpy.float64)

        for k in range(self.hs, self.nz+self.hs):
            for i in range(self.hs, self.nx+self.hs):
                r = self.state[i, k, self.ID_DENS] + self.hy_dens_cell[k] # density
                u = self.state[i, k, self.ID_UMOM] / r # u-wind
                w = self.state[i, k, self.ID_WMOM] / r # v-wind
                th = (self.state[i, k, self.ID_RHOT] + self.hy_dens_theta_cell[k]) / r # potential temperature (theta)
                p = self.c0 * math.pow(r*th, self.gamma) # pressure
                t = th / math.pow(self.p0/p, self.rd/self.cp) # temperature
                ke = r * (u*u*w*w) # kinetic energy
                ie = r * self.cv * t # internal energy
                rdc[0] += r * self.dx * self.dz # accumulate domain mass
                rdc[1] +=(ke + r * self.cv * t) * self.dx * self.dz # accumulate domain energy

        self.comm.Allreduce(rdc, buf)

        return buf[0], buf[1]

    def hydro_const_theta(self, z):

        t = self.theta0
        exner = self.exner0 - self.grav * z / (self.cp * self.theta0)
        p = self.p0 * math.pow(exner, self.cp/self.rd)
        rt = math.pow(p/self.c0, 1./self.gamma)
        r = rt / t

        return r, t

    def sample_ellipse_cosine(self, x , z , amp , x0 , z0 , xrad , zrad):
        dist = math.sqrt(math.pow((x-x0)/xrad, 2.) + math.pow((z-z0)/zrad, 2.)) * self.pi / 2.
        #If the distance from bubble center is less than the radius, create a cos**2 profile
        return (amp * math.pow(math.cos(dist), 2.)) if (dist <= self.pi / 2.) else 0.

    def thermal(self, x, z):

        hr, ht = self.hydro_const_theta(z)

        r = 0.
        t = 0.
        u = 0.
        w = 0.

        t = t + self.sample_ellipse_cosine(x, z, 3., self.xlen/2., 2000., 2000., 2000.)

        return r, u, w, t, hr, ht

    def output(self, etime):

        for k in range(self.hs, self.nz+self.hs):
            for i in range(self.hs, self.nx+self.hs):
                self.dens[i-self.hs, k-self.hs] = self.state[i, k, self.ID_DENS]
                self.uwnd[i-self.hs, k-self.hs] = (self.state[i, k, self.ID_UMOM] /
                                (self.hy_dens_cell[k] + self.state[i,k,self.ID_DENS]))
                self.wwnd[i-self.hs, k-self.hs] = (self.state[i, k, self.ID_WMOM] /
                                (self.hy_dens_cell[k] + self.state[i,k,self.ID_DENS]))
                self.theta[i-self.hs, k-self.hs] = ((self.state[i, k, self.ID_RHOT] +
                                self.hy_dens_theta_cell[k]) / (self.hy_dens_cell[k] +
                                self.state[i,k,self.ID_DENS]) - self.hy_dens_theta_cell[k] /
                                self.hy_dens_cell[k])

        # file name contains varname, ibeg, kbeg, count???``
        self.vdens.write(self.dens, (self.i_beg, self.k_beg))
        self.vuwnd.write(self.uwnd, (self.i_beg, self.k_beg))
        self.vwwnd.write(self.wwnd, (self.i_beg, self.k_beg))
        self.vtheta.write(self.theta, (self.i_beg, self.k_beg))

#        if self.is_master():
#            print("*** OUTPUT ***")
#            print("sum dens = %f" % self.dens.sum())
#            print("sum uwnd = %f" % self.uwnd.sum())
#            print("sum wwnd = %f" % self.wwnd.sum())
#            print("sum theta = %f" % self.theta.sum())

    def is_master(self):
        return self.rank == 0

def main():

    parser = argparse.ArgumentParser(description='Python porting of miniWeather')
    parser.add_argument('-x', '--nx', default=NX, type=int,
                        help='number of total grid cells in the x-dimension')
    parser.add_argument('-y', '--nz', default=NZ, type=int,
                        help='number of total grid cells in the z-dimension')
    parser.add_argument('-s', '--simtime', default=SIM_TIME,
                        type=float, help='total simulation time in seconds')
    parser.add_argument('-o', '--outfreq', default=OUT_FREQ,
                        type=float, help='frequency to perform output in seconds')
    parser.add_argument('-d', '--dataspec', default=DATA_SPEC,
                        help='which data initialization to use')

    argps = parser.parse_args()

    domain = LocalDomain(argps.nx, argps.nz, argps.dataspec)

    mass0, te0 = domain.reductions()

    etime = 0.

    domain.output(etime)

    output_counter = 0

    if domain.is_master():
        start_time = time.time()

    while etime < argps.simtime:

        if etime + domain.dt > argps.simtime:
            domain.dt = argps.simtime - etime

        domain.timestep()

        if domain.is_master():
            print("Elapsed Time: %10.3f / %10.3f" % (etime, argps.simtime))

        etime = etime + domain.dt 

        output_counter = output_counter + domain.dt 
        if output_counter >= argps.outfreq:
#
#            print("")
#            print("%d state sum: %f" % (self.rank, self.state.sum()))
#            print("%d state_tmp sum: %f" % (self.rank, self.state_tmp.sum()))
#            print("%d hy_dens_cell sum: %f" % (self.rank, self.hy_dens_cell.sum()))
#            print("%d hy_dens_theta_cell sum: %f" % (self.rank, self.hy_dens_theta_cell.sum()))
#            print("%d hy_dens_int sum: %f" % (self.rank, self.hy_dens_int.sum()))
#            print("%d hy_dens_theta_int sum: %f" % (self.rank, self.hy_dens_theta_int.sum()))
#            print("%d hy_pressure_int sum: %f" % (self.rank, self.hy_pressure_int.sum()))
#            print("%d tend sum: %f" % (self.rank, self.tend.sum()))
#            print("%d flux sum: %f" % (self.rank, self.flux.sum()))
#            sys.exit(0)
            output_counter = output_counter - argps.outfreq
            domain.output(etime)

    if domain.is_master():
        print("CPU Time: %f" % (time.time() - start_time))

    mass, te = domain.reductions()

    if domain.is_master():
        print("d_mass: %f" % ((mass - mass0)/mass0))
        print("d_te: %f" % ((te - te0)/te0))

    domain.slabs.close()


if __name__ == "__main__":
    sys.exit(main())
