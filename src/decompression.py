import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import pickle
import time
import multiprocessing
import math

"""""
def block_bwt(input, key, index, return_dict):
	outputBWT = sbwt.ibwt_from_suffix(input, key)
	return_dict[index] = outputBWT
"""""

def multi_rle_decode(task):
	input_block, index = task
	rleModule = rle.Rle()
	return index,rle.Rle.parallel_rle_decode(rleModule, data=input_block)

def block_bwt(task):
	input_block, key, index = task
	return index, sbwt.ibwt_from_suffix(input_block, key)

def decompressione(secret_key: str, mode: int):
	start = time.time()
	# leggo il dizionario salvato dalla bwt in fase di compressione
	dictionaryFile = open("TestFiles/Output/outputDictBWT.txt", "rb")
	dictionaryLines = dictionaryFile.read()
	dictionaryStr = dictionaryLines.decode()
	dictionaryFile.close()  
	# IPC
	pcStartTime = time.time()

	encodedFile = open("TestFiles/Output/outputPC.txt", "rb")
	if mode == 1:
		encoded = encodedFile.read()
	else:
		encoded = pickle.load(encodedFile)
	encodedFile.close() 
	outputPC = pc.decompress(encoded, mode)

	pcElapsedTime = time.time() - pcStartTime
	print(str(pcElapsedTime) + "  -> elapsed time of I-PC")
	#print("OUTPUT", outputPC[:500])

	# IRLE
	'''rleFile = open("TestFiles/Output/outputRLE.txt", "r")
	rleLines = rleFile.readlines()
	rleString = ""
	for val in rleLines:
		rleString += val'''
	rleStartTime = time.time()

	"******************************"
	#Parallel RLE Decoding 
	rleDecodedString = []
	nproc = multiprocessing.cpu_count()
	size = len(outputPC)

	MIN_BLOCK = 256 * 1024      # 256 KB grandezza L2 cache orientativa
	MAX_BLOCK = 2 * 1024 * 1024  # 2 MB grandezza L3 cache orientativa

	block_length = max(MIN_BLOCK, min(math.ceil(size / nproc),MAX_BLOCK))
	num_blocks = max(1, math.ceil(size / block_length)) #se il file è minore di MIN_BLOCK va in full size

	if num_blocks > 1:
		print("rle block mode")
		chunksize = min(3,max(1,num_blocks//nproc)) #Definisco la dimensione dei chunk per ogni processo


		time_start = time.time()
		tasks = []
		displ = []
		counts = []
		results = []
		size = nproc * 10
		
		displ.append(0)
		seek = block_length - 1
		j=0
		for i in range(num_blocks):
			if i < num_blocks - 1:
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

			# Preparo i task prendendo i blocchi di input in base a displ e counts
			input_block = outputPC[displ[i]:displ[i] + counts[i]]
			input_block = input_block[:-1] if input_block.endswith(",") else input_block #Rimuovo la virgola finale se presente
			tasks.append((input_block,j))
			j+=1	

		with  multiprocessing.Pool(nproc) as pool:
			results = pool.imap_unordered(multi_rle_decode, tasks, chunksize) # processa i task in parallelo, se finisce un task prende il successivo

			# ricostruzione ordinata
			output = [None] * j
			for index, res in results:
				output[index] = res

		rleDecodedString ="".join(output)
		rleDecodedString = rleDecodedString[:-1]  # rimuove l'ultima virgola aggiunta in parallel rle decode
	else:
		print("rle full file mode")
		rleModule = rle.Rle()
		rleDecodedString = rle.Rle.rle_decode(rleModule, data=outputPC)

	"******************************"
	#print(rleDecodedString)

	rleElapsedTime = time.time() - rleStartTime
	print(str(rleElapsedTime) + "  -> elapsed time of I-RLE")

	# IMTF
	
	mtfStartTime = time.time()

	block_size = 1024 #1/2((math.log2(len(stringInput))/math.log2(len(dictionary)))) The real formula is this one

	""" mtfList = rleDecodedString.split(",")
	res = []
	for i in mtfList:
		res.append(int(i)) """

	res = [int(i) for i in rleDecodedString.split(",") if i]
	#mtfDecodedString = mtf.decode(res, dictionary=sorted(dictionaryStr))
	mtfDecodedString = bmtf.secure_decode(res, sorted(dictionaryStr), secret_key, block_size)
	#print("-----MTF: " + mtfDecodedString)

	mtfElapsedTime = time.time() - mtfStartTime
	print(str(mtfElapsedTime) + "  -> elapsed time of I-BMTF")

	# IBWT

	bwtStartTime = time.time()

	# Dividi in blocchi mtfDecodedString
	#block_lenght = 1024*30 +1 # Deve essere la stessa usata in compressione +1 per l'EOF

	bFile = open("TestFiles/Output/bfile.txt", "r")
	block_lenght = int(bFile.readline()) + 1 #add EOF

	MIN_BLOCK = 256 * 1024      # 256 KB grandezza L2 cache orientativa
	MAX_BLOCK = 2 * 1024 * 1024  # 2 MB grandezza L3 cache orientativa

	fileSize = len(mtfDecodedString)
	#Ottego il numero di processori disponibili per dividere in blocchi la BWT e salvo
	
	#Block lenght può essere compresa solo tra MIN_BLOCK e MAX_BLOCK
	
	num_blocks = max(1, fileSize // block_lenght) #se il file è minore di MIN_BLOCK va in full size

	bFile.close()   
	using_blocks = True
	bwtDecodedString = []   
	rFile = open("TestFiles/Output/rfile.txt", "r")
	r = rFile.readline()
	rFile.close()
	
	nproc = multiprocessing.cpu_count()
	if num_blocks > 1:
		print("block mode")
		tasks = []
		chunksize = min(3,max(1,num_blocks//nproc)) #Definisco la dimensione dei chunk per ogni processo
		j = 0
		
		for i in range(0, len(mtfDecodedString), block_lenght):
			input_block = mtfDecodedString[i:i+block_lenght] 
			tasks.append((input_block, r + secret_key, j)) #ogni task è un blocco da processare e il suo indice j
			j += 1


		with  multiprocessing.Pool(nproc) as pool:
			results = pool.imap_unordered(block_bwt, tasks, chunksize) # processa i task in parallelo, se finisce un task prende il successivo

			# ricostruzione ordinata
			output = [None] * j
			for index, res in results:
				output[index] = res

			bwtDecodedString.extend("".join(output[i]) for i in range(0,j))
	else:
		print("full file mode")
		bwtDecodedString = sbwt.ibwt_from_suffix(mtfDecodedString, secret_key)

	#print(bwtDecodedString)
	outputBWTFile = open("TestFiles/Output/decompressed.txt", "wb")
	outputBWTString = "".join(bwtDecodedString)
	#outputBWTFile.write(str(outputBWTString))
	outputBWTFile.write(outputBWTString.encode())
	outputBWTFile.close()   
	bwtElapsedTime = time.time() - bwtStartTime
	print(str(bwtElapsedTime) + "  -> elapsed time of I-BWT")

	print(str(time.time() - start) + " -> elapsed time of decompression")
