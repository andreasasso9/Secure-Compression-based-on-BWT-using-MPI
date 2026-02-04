import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import time
import random
import math
from mpi4py import MPI
import numpy as np
import os
import datetime

def block_bwt(stringInput, key, rank):
    results = []
    strings = []
    # Split se > 2MB
    if len(stringInput) > 2*1024*1024:
        strings = [stringInput[i:i + 2*1024*1024] for i in range(0, len(stringInput), 2*1024*1024)]
    else:
        strings = [stringInput]

    with open(f"TestFiles/Output/bfile_{rank}.txt", "w") as bFile:
        for s in strings:
            s += "\003"
            bFile.write(str(len(s)) + "\n") # Salva la lunghezza reale con \003
            out_part = sbwt.bwt_from_suffix(s, key)
            results.append(out_part) 

    return "".join(results) # Unisce tutti i sotto-blocchi del rank

def log_progress(message, mode='a'):
	if MPI.COMM_WORLD.Get_rank() == 0:  # Solo il rank 0 esegue il log
		with open("progress.txt", mode) as progress:
			timestamp = datetime.datetime.now().strftime("%H:%M:%S")
			progress.write(f"[{timestamp}] {message}\n")

def compressione(file_name: str, secret_key: str, mode: int):
	comm = MPI.COMM_WORLD
	size = comm.Get_size()
	rank = comm.Get_rank()
	r = None
	filePath = "TestFiles/Input/" + file_name
	
	if rank == 0:
		start_time = time.time()

		#BWT
		print("starting sBWT...")
		log_progress("starting sBWT...", mode='w')
		bwtStartTime = time.time()

		# Salvo la chiave per la BWT
		r = str(random.randint(0, 9999999))
		rFile = open("TestFiles/Output/rfile.txt", "w")
		rFile.write(r)

	# Distribuisco i blocchi della BWT e la chiave r a tutti i processi
	r = comm.bcast(r, root=0)
	block_length = 0
	
	displ = np.zeros(size, dtype='i') if rank == 0 else None
	counts = np.zeros(size, dtype='i') if rank == 0 else None
	#sendbuf = []
	if rank == 0:
		fileSize = os.path.getsize(filePath)
		#sendbuf = np.frombuffer(stringInput.encode("utf-8"), dtype='b')
		# sendbuf = np.fromfile("file.txt", dtype='b')
		block_length = math.floor(fileSize / size)
		displ[0] = 0
		counts[0] = block_length
		# Calcolo counts e displ, counts contiene la lunghezza di ogni blocco, displ contiene gli spostamenti
		for i in range(1, size):
			if i < size - 1:
				counts[i] = block_length
			else:
				counts[i] = fileSize - block_length * (size - 1)  # ultimo processo prende il resto
			displ[i] = displ[i-1] + counts[i-1]  # offset cumulativo
			
	
	# Scatters the blocks to all processes
	time_start = time.time()
	block_length = comm.scatter(counts, root=0)
	recv_len = np.zeros(1, dtype='i')
	offset = np.zeros(1, dtype='i')

	comm.Scatter(counts, recv_len, root=0)
	comm.Scatter(displ, offset, root=0)

	with open(filePath, "rb") as f:
		f.seek(offset[0])
		data = f.read(recv_len[0])

	stringInput = data.decode()

	eelapsed_time = time.time() - time_start
	if not rank: 
		print(str(eelapsed_time) + "  -> elapsed time of encoding BWT scatter")
		log_progress(str(eelapsed_time) + "  -> elapsed time of encoding BWT scatter")

	time_start = time.time()
	#outputBWT_block = sbwt.bwt_from_suffix(stringInput, r + secret_key)
	
	outputBWT_block = block_bwt(stringInput, r + secret_key, rank)
	eelapsed_time = time.time() - time_start
	if not rank: 
		print(str(eelapsed_time) + "  -> elapsed time of sBWT block")
		log_progress(str(eelapsed_time) + "  -> elapsed time of sBWT block")

	time_start = time.time()
	outputBWT_block_list = comm.gather(outputBWT_block, root=0)
	eelapsed_time = time.time() - time_start
	if not rank: 
		print(str(eelapsed_time) + "  -> elapsed time of gathering BWT blocks")
		log_progress(str(eelapsed_time) + "  -> elapsed time of gathering BWT blocks")

	rleModule = rle.Rle()
	outputMTF = []
	# Salvo l'output della BWT e il dizionario
	if rank == 0:
		outputBWT = ""
		for element in outputBWT_block_list:
			if element is not None:
				outputBWT += element
		bwtElapsedTime = time.time() - bwtStartTime
		print(str(bwtElapsedTime) + "  -> elapsed time of sBWT")
		log_progress(str(bwtElapsedTime) + "  -> elapsed time of sBWT")
		fileOutputBWT = open("TestFiles/Output/outputBWT.txt", "w+", encoding="utf-8")
		fileOutputBWT.write(outputBWT)
		#salvo il dizionario della BWT
		fileOutputDictBWT = open("TestFiles/Output/outputDictBWT.txt", "wb")
		dictStr = ""
		for element in set(outputBWT):
			dictStr += element
		fileOutputDictBWT.write(dictStr.encode())

		#MTF
		print("starting bMTF...")
		log_progress("starting bMTF...")

		dictionary = sorted(dictStr)

		MIN_BLOCK_MTF = 10 * 1024      # 10 KB per non diminuire troppo la sicurezza crittografica
		MAX_BLOCK_MTF = 2 * 1024 * 1024  # 2 MB grandezza L3 cache orientativa

		block_size = max(MIN_BLOCK_MTF, min((len(outputBWT) // len(dictionary)),MAX_BLOCK_MTF))
		with open("TestFiles/Output/bFileMTF.txt", "w", encoding='utf-8') as bwtFile:
			bwtFile.write(str(block_size))
		
		
		mtf_start_time = time.time()
		#print(sorted(dictionary))
		#outputMTF = mtf.encode(plain_text=outputBWT, dictionary=sorted(dictionary)) 
		outputMTF = bmtf.secure_encode(outputBWT, dictionary, secret_key, block_size)
		mtf_elapsed_time = time.time() - mtf_start_time
		print(str(mtf_elapsed_time) + "  -> elapsed time of bMTF")
		log_progress(str(mtf_elapsed_time) + "  -> elapsed time of bMTF")

		#RLE
		print("starting RLE")
		log_progress("starting RLE")
		
		rle_start_time = time.time()

		####################################àoutputMTF=list(map(str, outputMTF))

		outputMTF = np.array(outputMTF, dtype=np.uint32)
		counts = []
		displ = []

		displ.append(0)
		base_count = math.floor(len(outputMTF) / size)
		for i in range(size):
			""" if i < size - 1:
				counts.append(base_count)
			else:
				counts.append(len(outputMTF) - displ[i])  # ultimo processo prende il resto """
			counts.append( base_count if i < size - 1 else len(outputMTF) - displ[i] )
			displ.append(displ[i] + counts[i])

		displ = displ[:-1]  # rimuovo l'ultimo elemento che è in più
			

	outputMTFBlockLength = comm.scatter(counts, root=0)
	
	recvbuf = np.empty(outputMTFBlockLength, dtype=np.uint32)
	
	comm.Scatterv((outputMTF, counts, displ, MPI.UINT32_T), recvbuf, root=0)

	outputRLEBlock = rle.Rle.parallel_rle_encode(rleModule, data=recvbuf)

	outputRLE_list = comm.gather(outputRLEBlock, root=0)

	if rank == 0:
		outputRLE = rleModule.rle_merge(outputRLE_list)
		

		rle_elapsed_time = time.time() - rle_start_time
		print(str(rle_elapsed_time) + "  -> elapsed time of RLE")
		log_progress(str(rle_elapsed_time) + "  -> elapsed time of RLE")
		fileOutputRLE = open("TestFiles/Output/outputRLE.txt", "w+")
		fileOutputRLE.write(str(outputRLE))
		fileOutputRLE.close()

		#PC
		print("starting PC")
		log_progress("starting PC")
		pc_start_time = time.time()
		pc.compress(outputRLE, mode)
		pc_elapsed_time = time.time() - pc_start_time
		print(str(pc_elapsed_time) + "  -> elapsed time of PC")
		log_progress(str(pc_elapsed_time) + "  -> elapsed time of PC")

		total_elapsed_time = time.time() - start_time
		print(str(total_elapsed_time) + "  -> elapsed time of compression")
		log_progress(str(total_elapsed_time) + "  -> elapsed time of compression")
