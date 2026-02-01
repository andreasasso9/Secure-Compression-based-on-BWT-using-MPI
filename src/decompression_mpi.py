import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import pickle
import time
import math
import numpy as np
from mpi4py import MPI
import datetime

def block_ibwt(input_block, key, rank):
    with open(f"TestFiles/Output/bfile_{rank}.txt", "r") as bFile:
        block_lengths = [int(line.strip()) for line in bFile.readlines()]
    
    final_output = []
    offset = 0
    for length in block_lengths:
        # Estrai esattamente la porzione di blocco corretta
        s = input_block[offset : offset + length]
        offset += length
        
        # Inverti la BWT
        out = sbwt.ibwt_from_suffix(s, key)
        final_output.append("".join(out))
        
    return "".join(final_output)
		

def log_progress(message):
    if MPI.COMM_WORLD.Get_rank() == 0:  # Solo il rank 0 esegue il log
        with open("progress.txt", "a") as progress:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            progress.write(f"[{timestamp}] {message}\n")

def decompressione(secret_key: str, mode: int):
	comm = MPI.COMM_WORLD
	size = comm.Get_size()
	rank = comm.Get_rank()
	rFile = open("TestFiles/Output/rfile.txt", "r")
	r = rFile.readline()

	displ = []
	counts = []
	sendbuf = []
	if rank == 0:
		start = time.time()
		# leggo il dizionario salvato dalla bwt in fase di compressione
		dictionaryFile = open("TestFiles/Output/outputDictBWT.txt", "rb")
		dictionaryLines = dictionaryFile.read()
		dictionaryStr = dictionaryLines.decode()

		# IPC
		pcStartTime = time.time()

		encodedFile = open("TestFiles/Output/outputPC.txt", "rb")
		if mode == 1:
			encoded = encodedFile.read()
		else:
			encoded = pickle.load(encodedFile)

		outputPC = pc.decompress(encoded, mode)

		pcElapsedTime = time.time() - pcStartTime
		print(str(pcElapsedTime) + "  -> elapsed time of I-PC")
		log_progress(str(pcElapsedTime) + "  -> elapsed time of I-PC")

		# IRLE
		'''rleFile = open("TestFiles/Output/outputRLE.txt", "r")
		rleLines = rleFile.readlines()
		rleString = ""
		for val in rleLines:
			rleString += val'''
		rleStartTime = time.time()


		# Accede a una block_length approssimata per non scorrrere l'intero outputPC
		
		block_length = math.floor(len(outputPC) / size)
		print("Block length RLE: ", block_length)
		log_progress(f"Block length RLE: {block_length}")
		displ.append(0)
		seek = block_length - 1
		for i in range(size):
			if i < size - 1:
				# Cerca l'ultima virgola nel blocco per non spezzare i numeri
				while True:
					if outputPC[seek] == ',':
						counts.append(seek + 1 - displ[i]) 
						displ.append(displ[i] + counts[i])
						seek += block_length
						break
					else:
						seek -= 1
			else:
				# Ultimo processo prende il resto
				remaining = len(outputPC) - sum(counts)
				counts.append(remaining)
	
		sendbuf = np.frombuffer(outputPC.encode('utf-8'), dtype='b')
	# Scatters the blocks to all processes

	block_length = comm.scatter(counts, root=0)
	recvbuf = np.empty(block_length, dtype='b')
	comm.Scatterv((sendbuf, counts, displ, MPI.BYTE), recvbuf, root=0)
	recvbuf = recvbuf.tobytes().decode("utf-8")
	recvbuf = recvbuf[:-1] if recvbuf.endswith(",") else recvbuf  # rimuove la virgola rimasta dalla divisione dei blocchi

	recvbuf = recvbuf[:-1] if recvbuf.endswith(",") else recvbuf  # rimuove la virgola rimasta dalla divisione dei blocchi

	# --- INIZIO NUOVO BLOCCO OTTIMIZZATO ---

	rleModule = rle.Rle()

	# 1. Decodifica RLE locale (produce stringa CSV "10,20,30...")
	rleDecodedStringBlock = rle.Rle.parallel_rle_decode(rleModule, data=recvbuf)

	# 2. Conversione PARALLELA in Interi
	# Invece di mandare stringhe giganti al master, convertiamo subito in numeri.
	# "if x" filtra le stringhe vuote causate da virgole doppie o finali,
	# RISOLVENDO DEFINITIVAMENTE IL "ValueError: invalid literal for int()".
	local_integers = [int(x) for x in rleDecodedStringBlock.split(',') if x]
	
	# Convertiamo in numpy array per efficienza nel trasferimento
	local_array = np.array(local_integers, dtype=np.int32)

	# 3. Raccogliamo ARRAY, non stringhe
	# Gather restituisce una lista di numpy arrays [arr_rank0, arr_rank1, ...]
	gathered_arrays = comm.gather(local_array, root=0)

	if rank == 0:
		# Uniamo tutti gli array numpy in uno solo (molto veloce)
		res = np.concatenate(gathered_arrays)
		
		# Convertiamo in lista standard Python (se bmtf richiede lista standard)
		# Se bmtf accetta numpy array, puoi rimuovere .tolist()
		res = res.tolist()

		rleElapsedTime = time.time() - rleStartTime
		print(str(rleElapsedTime) + "  -> elapsed time of I-RLE (Parallel Int Conversion)")
		log_progress(str(rleElapsedTime) + "  -> elapsed time of I-RLE (Parallel Int Conversion)")

		# IMTF
		mtfStartTime = time.time()

		# Non serve più il ciclo for lento di conversione, 'res' è già pronto!
		block_size = 1024 
		mtfDecodedString = bmtf.secure_decode(res, sorted(dictionaryStr), secret_key, block_size)

		mtfElapsedTime = time.time() - mtfStartTime
		print(str(mtfElapsedTime) + "  -> elapsed time of I-BMTF")
		log_progress(str(mtfElapsedTime) + "  -> elapsed time of I-BMTF")

		# IBWT
		bwtStartTime = time.time()

	# --- FINE NUOVO BLOCCO OTTIMIZZATO ---
	# Distribuisco i blocchi della BWT
	block_length = 0
	displ = []
	counts = []
	sendbuf = []
	if rank == 0:
		sendbuf = np.frombuffer(mtfDecodedString.encode("utf-8"), dtype='b')
		block_length = math.floor(len(sendbuf) / size)
		displ.append(0)
		# Calcolo counts e displ, counts contiene la lunghezza di ogni blocco, displ contiene gli spostamenti
		for i in range(size):
			if i < size - 1:
				counts.append(block_length)
				displ.append(displ[i] + counts[i])
			else:
				counts.append(len(sendbuf) - block_length * (size - 1))
	block_length = comm.scatter(counts, root=0)
	recvbuf = np.empty(block_length, dtype='b')
	comm.Scatterv((sendbuf, counts, displ, MPI.BYTE), recvbuf, root=0)
	recvbuf = recvbuf.tobytes().decode("utf-8")
	outputBWT_block = block_ibwt(recvbuf, r + secret_key, rank)
	outputBWT_block = ''.join(outputBWT_block)
	outputBWT_block_list = comm.gather(outputBWT_block, root=0)

	if rank == 0:
		outputBWTFile = open("TestFiles/Output/decompressed.txt", "wb")
		outputBWTString = ""
		for element in outputBWT_block_list:
			if element is not None:
				outputBWTString += element
		outputBWTFile.write(outputBWTString.encode())
		bwtElapsedTime = time.time() - bwtStartTime
		print(str(bwtElapsedTime) + "  -> elapsed time of I-BWT")
		log_progress(str(bwtElapsedTime) + "  -> elapsed time of I-BWT")

		print(str(time.time() - start) + " -> elapsed time of decompression")
		log_progress(str(time.time() - start) + " -> elapsed time of decompression")
