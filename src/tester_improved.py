import compression_improved as compression
import decompression_improved as decompression
import sys
import filecmp
from mpi4py import MPI

if __name__ == "__main__":	

	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()

	if len(sys.argv) < 4:
		if not rank: print("Missing arguments, proceeding with default ones\nname=alice29.txt, key=Chiave Segreta and mode = 0" )
		file_name = "manual.ps"
		secret_key = "Chiave segreta"
		mode = 0
	else:
		file_name = sys.argv[1]
		secret_key = sys.argv[2]
		mode = int(sys.argv[3])
	if not rank: print("starting compression...")
	compression.compressione(file_name, secret_key, mode)
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	comm.barrier()

	if not rank: print("\n\nstarting decompression...")
	decompression.decompressione(secret_key, mode)
	if not rank:
		original_file_path = "TestFiles/Input/" + file_name
		decompressed_file_path = "TestFiles/Output/decompressed.txt"
		equals = filecmp.cmp(original_file_path, decompressed_file_path, False)
		if equals:
			print("decompression was successful")
		else:
			print("decompression failed")