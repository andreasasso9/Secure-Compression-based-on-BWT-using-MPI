import compression_mpi as compression
import decompression_mpi as decompression
import sys
import filecmp
from mpi4py import MPI
import os

if __name__ == "__main__":	

	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()

	if len(sys.argv) < 4:
		if not rank: print("Missing arguments, proceeding with default ones\nname=alice29.txt, key=Chiave Segreta and mode = 0" )
		file_name = "alice29.txt"
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

		orig = os.path.getsize(original_file_path)
		comp = os.path.getsize("TestFiles/Output/outputPC.txt")

		ratio = orig / comp
		percent = (1.0 - (comp / orig)) * 100

		print(f"Compression ratio: {ratio:.4f}")
		print(f"Compressione: {percent:.2f}%")

		print(f"Originale: {orig} bytes")
		print(f"Compresso: {comp} bytes")