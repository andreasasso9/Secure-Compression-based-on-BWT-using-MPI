# Secure Parallel Compression: sBWT + DC3 + MPI

Implementazione ad alte prestazioni di un algoritmo di compressione sicura basato sulla **Scrambled Burrows-Wheeler Transform (sBWT)**.

Questo progetto migliora un'implementazione preesistente introducendo l'algoritmo lineare **DC3** per la costruzione del Suffix Array e sfruttando il calcolo parallelo tramite **Multiprocessing** e **MPI**.

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
```bash
mpiexec -n<num_processi> python tester_mpi.py <file_input> <chiave_segreta> <codifica>
```

# 3. Esecuzione Bzip
```bash
python tester_bzip.py <file_input>
```
