import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import pickle
import time
import multiprocessing
import math
import numpy as np
from mpi4py import MPI

def block_bwt(input, key, index, return_dict):
    output = sbwt.ibwt_from_suffix(input, key)
    return_dict[index] = output


def decompressione(secret_key: str, mode: int):
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    rFile = open("TestFiles/Output/rfile.txt", "r")
    r = rFile.readline()
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
    outputBWT_block = sbwt.ibwt_from_suffix(recvbuf, r + secret_key)
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

        print(str(time.time() - start) + " -> elapsed time of decompression")
