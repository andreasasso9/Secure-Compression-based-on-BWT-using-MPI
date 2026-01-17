from mpi4py import MPI
import numpy as np
import math

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

sendbuf = None
chunks = None
block_length = 0
displ = []
counts = []
if rank == 0:
	
	sendbuf = 'a'*20
	
		#sendbuf += '\0' * (size - len(sendbuf) % size)  # padding to make it divisible by size
	sendbuf = np.frombuffer(sendbuf.encode("utf-8"), dtype='b')
	block_length = math.floor(len(sendbuf) / size)
	displ.append(0)
	for i in range(size):
		if i < size - 1:
			counts.append(block_length)
			displ.append(displ[i] + counts[i])
		else:
			counts.append(len(sendbuf) - block_length * (size - 1))
		
	

	
	print("Block length: ", block_length)

block_length = comm.scatter(counts, root=0)
recvbuf = np.empty(block_length, dtype='b')
comm.Scatterv((sendbuf, counts, displ, MPI.BYTE), recvbuf, root=0)
recvbuf = recvbuf.tobytes().decode("utf-8").rstrip('\0')
print("Rank ", rank, " received ", recvbuf)
