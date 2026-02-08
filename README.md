# Secure Compression based on sBWT using MPI

Implementazione di un algoritmo di compressione sicura basato sulla **Scrambled Burrows-Wheeler Transform (sBWT)** e **Blocky Move-to-Front**.

Questo progetto migliora un'implementazione preesistente introducendo l'algoritmo lineare **DC3** per la costruzione del Suffix Array e sfruttando il calcolo parallelo tramite **Multiprocessing** e **MPI**.

Il progetto implementa una pipeline di compressione sicura basata sulla trasformata sBWT e sull'algoritmo bMTF per ottimizzare il rateo di compressione tramite **Run-Length Encoding (RLE)**. Infine, i dati vengono codificati con un algoritmo a lunghezza variabile selezionabile.

Progetto realizzato per il corso di *Compressione Dati*, Corso di Laurea Magistrale in Informatica, Università degli Studi di Salerno.

### Librerie Python
Installa le dipendenze tramite `pip`:
```bash
pip install -r requirements.txt
```

## 1. Esecuzione Multiprocessing
```bash
python tester.py <file_input> <chiave_segreta> <codifica>
```

## 2. Esecuzione MPI
Installazione MPI:  
Windows: https://learn.microsoft.com/it-it/message-passing-interface/microsoft-mpi  
Linux: https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/downloading.html  

Esecuzione:
```bash
mpiexec -n <num_processi> python tester_mpi.py <file_input> <chiave_segreta> <codifica>
mpirun -n <num_processi> python tester_mpi.py <file_input> <chiave_segreta> <codifica>
```

Il parametro 'chiave segreta' è una stringa che funge da chiave per la crittografia simmetrica integrata nel processo di compressione che "mescola" i dati in modo che possano essere decompressi solo da chi possiede questa chiave.
Il parametro codifica seleziona quale algoritmo di compressione utilizzare nell'ultima fase del processo:  
0 = Huffman  
1 = Arithmetic Coding  
2 = LZW  
3 = BZip2  

# 3. Esecuzione Bzip
```bash
python tester_bzip.py <file_input>
```
