import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import time
import multiprocessing
import random
import math
import os
import numpy as np
from multiprocessing import RawArray

"""""
def block_bwt(input, key, index, return_dict):
	outputBWT = sbwt.bwt_from_suffix(input, key)
	return_dict[index] = outputBWT
"""""
shared_arr = None  # globale

def init_worker(arr):
    global shared_arr
    shared_arr = arr  # ogni processo vede lo stesso buffer

def block_bwt(args):
	stringInput, key, index = args
	
	stringInput += "\003"
	return index, sbwt.bwt_from_suffix(stringInput, key)

def multi_rle_encode(task):
	global shared_arr
	start, end, index = task
	arr = np.frombuffer(shared_arr, dtype=np.int32)

	block = arr[start:end]  # ogni processo legge solo il suo blocco
	if len(block) == 0:  # protezione contro blocchi vuoti
		return index, ""
	block = block[:-1] if block[-1] == "," else block  # rimuovi virgola se presente
	rleModule = rle.Rle()
	rle_result = rle.Rle.parallel_rle_encode(rleModule,data=list(map(str, block)))

	return index, rle_result



def compressione(file_name: str, secret_key: str, mode: int):
	filePath = "TestFiles/Input/" + file_name
	   
	start_time = time.time()
	#BWT
	print("starting sBWT...")
	bwtStartTime = time.time()


	#*********************************#
	# Codice per eseguire la BWT a blocchi

	#block_lenght = 1024*30
	outputBWT = ""

	#Salvo la chiave per la BWT
	r = str(random.randint(0, 9999999))
	rFile = open("TestFiles/Output/rfile.txt", "w")
	rFile.write(r)
	rFile.close()

	nproc = multiprocessing.cpu_count()
	MIN_BLOCK = 256 * 1024      # 256 KB grandezza L2 cache orientativa
	MAX_BLOCK = 2 * 1024 * 1024  # 2 MB grandezza L3 cache orientativa

	fileSize = os.path.getsize(filePath)
	#Ottego il numero di processori disponibili per dividere in blocchi la BWT e salvo
	
	#Block lenght può essere compresa solo tra MIN_BLOCK e MAX_BLOCK
	block_length = max(MIN_BLOCK, min((fileSize // nproc),MAX_BLOCK))
	num_blocks = max(1, fileSize // block_length) #se il file è minore di MIN_BLOCK va in full size

	print("Block length for BWT: ", block_length)
	bFile = open("TestFiles/Output/bfile.txt", "w")
	bFile.write(str(block_length))
	bFile.close()

	with open(filePath, "rb") as inputFile: 
		listInput = inputFile.read()
		stringInput = listInput.decode()
		inputFile.close()

	if num_blocks > 1:
		size = len(stringInput)
		print("block mode")
		time_start = time.time()

		chunksize = min(3,max(1,num_blocks//nproc)) #Definisco la dimensione dei chunk per ogni processo

		# preparo i task
		tasks = []
		j = 0

		for offset in range(0, size, block_length):
			input_block = stringInput[offset:offset+ block_length]
			tasks.append((input_block, r + secret_key, j))
			j += 1
		
		with  multiprocessing.Pool(nproc) as pool:
			results = pool.imap_unordered(block_bwt, tasks, chunksize) # processa i task in parallelo, se finisce un task prende il successivo

			# ricostruzione ordinata
			output = [None] * j
			for index, res in results:
				output[index] = res

		outputBWT = "".join(output)

		eelapsed_time = time.time() - time_start    
		print(str(eelapsed_time) + "  -> elapsed time of sBWT blocks")
	else:
		print("full file mode")
		stringInput += "\003" # Add EOF
		outputBWT = sbwt.bwt_from_suffix(stringInput, secret_key)

	bwtElapsedTime = time.time() - bwtStartTime
	print(str(bwtElapsedTime) + "  -> elapsed time of sBWT")
	fileOutputBWT = open("TestFiles/Output/outputBWT.txt", "w+",encoding='utf-8')
	fileOutputBWT.write(outputBWT)
	fileOutputBWT.close()
	#salvo il dizionario della BWT
	fileOutputDictBWT = open("TestFiles/Output/outputDictBWT.txt", "wb")
	dictStr = ""
	for element in set(outputBWT):
		dictStr += element
	fileOutputDictBWT.write(dictStr.encode())
	fileOutputDictBWT.close()   

	#*********************************#
	#MTF
	print("starting bMTF...")
	dictionary = sorted(dictStr)

	#block_size = 1024 # 1/2((math.log2(len(stringInput))/math.log2(len(dictionary)))) The real formula is this one
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
	fileOutputMTF = open("TestFiles/Output/outputMTF.txt", "w+", encoding='utf-8')
	fileOutputMTF.write(str(outputMTF).replace(" ", ""))
	fileOutputMTF.close()   
	#*********************************#

	#RLE
	print("starting RLE")
	rle_start_time = time.time()
	size = len(outputMTF)

	block_length = max(MIN_BLOCK, min(math.ceil(size / nproc),MAX_BLOCK))
	num_blocks = max(1, math.ceil(size / block_length)) #se il file è minore di MIN_BLOCK va in full size


	if  num_blocks > 1:
		print("block mode")
		# outputMTF è lista di interi

		shared_arr = RawArray('i', size)  # interi condivisi
		shared_np = np.frombuffer(shared_arr, dtype=np.int32)
		shared_np += outputMTF # copia i dati nella matrice condivisa

		chunksize = min(3,max(1,num_blocks//nproc)) #Definisco la dimensione dei chunk per ogni processo
		time_start = time.time()
	   
		# preparo i task
		tasks = []
		j = 0

		#Assegno i blocchi ai task
		for i in range(num_blocks):
			start = i * block_length
			count = min(block_length, len(outputMTF) - start)
			end = start + count
			tasks.append((start, end, j))
			j+=1
		
		with  multiprocessing.Pool(processes=nproc, initializer=init_worker, initargs=(shared_arr,)) as pool:
			results = pool.imap_unordered(multi_rle_encode, tasks, chunksize) # processa i task in parallelo, se finisce un task prende il successivo

			# ricostruzione ordinata
			output = [None] * j
			for index, res in results:
				output[index] = res

		rleModule = rle.Rle()
		outputRLE = rleModule.rle_merge(output)

	else:
		print("rle full file mode")
		rleModule = rle.Rle()
		outputRLE = rle.Rle.rle_encode(rleModule, data=list(map(str, outputMTF))) # trasformo la lista di interi in lista di stringhe
	
	
   
	
	rle_elapsed_time = time.time() - rle_start_time
	print(str(rle_elapsed_time) + "  -> elapsed time of RLE")
	fileOutputRLE = open("TestFiles/Output/outputRLE.txt", "w+")
	fileOutputRLE.write(str(outputRLE))
	fileOutputRLE.close()


	#*********************************#
	#PC
	print("starting PC")
	pc_start_time = time.time()
	pc.compress(outputRLE, mode)
	pc_elapsed_time = time.time() - pc_start_time
	print(str(pc_elapsed_time) + "  -> elapsed time of PC")
	total_elapsed_time = time.time() - start_time
	print(str(total_elapsed_time) + "  -> elapsed time of compression")
	#*********************************#


