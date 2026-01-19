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
    
    rleModule = rle.Rle()
    rleDecodedString = rle.Rle.rle_decode(rleModule, data=outputPC)
    #print(rleDecodedString)

    rleElapsedTime = time.time() - rleStartTime
    print(str(rleElapsedTime) + "  -> elapsed time of I-RLE")

    # IMTF
    
    mtfStartTime = time.time()

    block_size = 1024 #1/2((math.log2(len(stringInput))/math.log2(len(dictionary)))) The real formula is this one

    mtfList = rleDecodedString.split(",")
    res = []
    for i in mtfList:
        res.append(int(i))
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
    bFile.close()   
    using_blocks = True
    bwtDecodedString = []   
    rFile = open("TestFiles/Output/rfile.txt", "r")
    r = rFile.readline()
    rFile.close()   
    if using_blocks and len(mtfDecodedString) > block_lenght:
        print("block mode")
        nproc = multiprocessing.cpu_count()
        tasks = []
        num_tasks = len(mtfDecodedString) // block_lenght
        chunksize = max(1, num_tasks // (nproc * 2)) #Definisco la dimensione dei chunk per ogni processo 
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
    outputBWTString = ""
    for i in range(0, len(bwtDecodedString)):
        outputBWTString += bwtDecodedString[i]
    #outputBWTFile.write(str(outputBWTString))
    outputBWTFile.write(outputBWTString.encode())
    outputBWTFile.close()   
    bwtElapsedTime = time.time() - bwtStartTime
    print(str(bwtElapsedTime) + "  -> elapsed time of I-BWT")

    print(str(time.time() - start) + " -> elapsed time of decompression")
