import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import time
import multiprocessing
import random
import subprocess
import math
from mpi4py import MPI
import numpy as np

def block_bwt(input, key, index, return_dict):
	outputBWT = sbwt.bwt_from_suffix(input, key)
	return_dict[index] = outputBWT

def compressione(file_name: str, secret_key: str, mode: int):
	comm = MPI.COMM_WORLD
	size = comm.Get_size()
	rank = comm.Get_rank()
	r = None
	if rank == 0:
		filePath = "TestFiles/Input/" + file_name
		inputFile = open(filePath, "rb")
	
		listInput = inputFile.read()
		stringInput = listInput.decode()

		
		start_time = time.time()

		#BWT
		print("starting sBWT...")
		bwtStartTime = time.time()

		# Salvo la chiave per la BWT
		r = str(random.randint(0, 9999999))
		rFile = open("TestFiles/Output/rfile.txt", "w")
		rFile.write(r)

	# Distribuisco i blocchi della BWT e la chiave r a tutti i processi
	r = comm.bcast(r, root=0)
	block_length = 0
	displ = []
	counts = []
	sendbuf = []
	if rank == 0:

		
		enc_time = time.time()

		sendbuf = np.frombuffer(stringInput.encode("utf-8"), dtype='b')
		# sendbuf = np.fromfile("file.txt", dtype='b')
		block_length = math.floor(len(sendbuf) / size)
		displ.append(0)
		# Calcolo counts e displ, counts contiene la lunghezza di ogni blocco, displ contiene gli spostamenti
		for i in range(size):
			if i < size - 1:
				counts.append(block_length)
				displ.append(displ[i] + counts[i])
			else:
				counts.append(len(sendbuf) - block_length * (size - 1))
		enc_elapsed_time = time.time() - enc_time
		if not rank: print(str(enc_elapsed_time) + "  -> elapsed time of encoding BWT distribution")
	
	# Scatters the blocks to all processes
	time_start = time.time()
	block_length = comm.scatter(counts, root=0)
	recvbuf = np.empty(block_length, dtype='b')
	comm.Scatterv((sendbuf, counts, displ, MPI.BYTE), recvbuf, root=0)
	recvbuf = recvbuf.tobytes().decode("utf-8")
	recvbuf+= "\003"
	eelapsed_time = time.time() - time_start
	if not rank: print(str(eelapsed_time) + "  -> elapsed time of encoding BWT scatter")

	time_start = time.time()
	outputBWT_block = sbwt.bwt_from_suffix(recvbuf, r + secret_key)
	eelapsed_time = time.time() - time_start
	if not rank: print(str(eelapsed_time) + "  -> elapsed time of sBWT block")
	
	time_start = time.time()
	outputBWT_block_list = comm.gather(outputBWT_block, root=0)
	eelapsed_time = time.time() - time_start
	if not rank: print(str(eelapsed_time) + "  -> elapsed time of gathering BWT blocks")

	# Salvo l'output della BWT e il dizionario
	if rank == 0:
		outputBWT = ""
		for element in outputBWT_block_list:
			if element is not None:
				outputBWT += element
		bwtElapsedTime = time.time() - bwtStartTime
		print(str(bwtElapsedTime) + "  -> elapsed time of sBWT")
		fileOutputBWT = open("TestFiles/Output/outputBWT.txt", "w+")
		fileOutputBWT.write(outputBWT)
		#salvo il dizionario della BWT
		fileOutputDictBWT = open("TestFiles/Output/outputDictBWT.txt", "wb")
		dictStr = ""
		for element in set(outputBWT):
			dictStr += element
		fileOutputDictBWT.write(dictStr.encode())

		#MTF
		print("starting bMTF...")
		
		dictionary = sorted(dictStr)
		block_size = 1024 # 1/2((math.log2(len(stringInput))/math.log2(len(dictionary)))) The real formula is this one
		
		mtf_start_time = time.time()
		#print(sorted(dictionary))
		#outputMTF = mtf.encode(plain_text=outputBWT, dictionary=sorted(dictionary)) 
		outputMTF = bmtf.secure_encode(outputBWT, dictionary, secret_key, block_size)
		mtf_elapsed_time = time.time() - mtf_start_time
		print(str(mtf_elapsed_time) + "  -> elapsed time of bMTF")
		fileOutputMTF = open("TestFiles/Output/outputMTF.txt", "w+")
		fileOutputMTF.write(str(outputMTF).replace(" ", ""))

		#RLE
		print("starting RLE")
		rleModule = rle.Rle()
		rle_start_time = time.time()
		outputRLE = rle.Rle.rle_encode(rleModule, data=list(map(str, outputMTF))) # trasformo la lista di interi in lista di stringhe
		rle_elapsed_time = time.time() - rle_start_time
		print(str(rle_elapsed_time) + "  -> elapsed time of RLE")
		fileOutputRLE = open("TestFiles/Output/outputRLE.txt", "w+")
		fileOutputRLE.write(str(outputRLE))
		fileOutputRLE.close()

		#PC
		print("starting PC")
		pc_start_time = time.time()
		pc.compress(outputRLE, mode)
		pc_elapsed_time = time.time() - pc_start_time
		print(str(pc_elapsed_time) + "  -> elapsed time of PC")
		total_elapsed_time = time.time() - start_time
		print(str(total_elapsed_time) + "  -> elapsed time of compression")
